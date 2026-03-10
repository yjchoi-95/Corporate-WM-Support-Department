from __future__ import annotations

from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo
import html
import io
import re
import time
import warnings
import zipfile

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import requests


warnings.filterwarnings("ignore", message="Unverified HTTPS request")

LIST_API_URL = "https://opendart.fss.or.kr/api/list.json"
PIIC_API_URL = "https://opendart.fss.or.kr/api/piicDecsn.json"
CB_API_URL = "https://opendart.fss.or.kr/api/cvbdIsDecsn.json"
BW_API_URL = "https://opendart.fss.or.kr/api/bwbdIsDecsn.json"
DOC_API_URL = "https://opendart.fss.or.kr/api/document.xml"

TARGET_REPORT_TITLES = {
    "주요사항보고서(유상증자결정)",
    "주요사항보고서(전환사채권발행결정)",
    "주요사항보고서(신주인수권부사채발행결정)",
}

OUT_RENAME_MAP = {
    "corp_name": "회사명",
    "ic_mthn": "증자방식",
    "fdpp_sum": "발행금액",
    "nstk_ps": "발행가액",
    "nstk_sum": "발행주식수",
    "본점소재지": "주소",
}

OUT_COLUMNS = [
    "회사명",
    "증자방식",
    "발행주식수",
    "발행가액",
    "발행금액",
    "납입일",
    "신주상장예정일",
    "대표이사",
    "주소",
    "작성책임자_직책",
    "작성책임자_성명",
    "작성책임자_전화번호",
    "URL",
]

CB_BW_OUT_COLUMNS = [
    "회사명",
    "사채종류",
    "사채총액",
    "청약일",
    "납입일",
    "만기일",
    "전환비율",
    "전환가액",
    "전환청구시작일",
    "대표이사",
    "주소",
    "작성책임자_직책",
    "작성책임자_성명",
    "작성책임자_전화번호",
    "URL",
]


def normalize_report_nm(report_nm: str) -> str:
    s = str(report_nm)
    s = re.sub(r"^\s*(\[[^\]]+\]\s*)+", "", s)
    return s.strip()


def to_numeric_series(series: pd.Series) -> pd.Series:
    return (
        series.replace("-", "0")
        .replace(",", "", regex=True)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )


def normalize_numeric_series(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": None, "-": None, "nan": None, "None": None})
        .pipe(pd.to_numeric, errors="coerce")
    )


def normalize_date_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(r"[.\-/]", "", regex=True)
    s = s.str.replace(r"\s+", "", regex=True)
    s = (
        s.str.replace("년", "", regex=False)
        .str.replace("월", "", regex=False)
        .str.replace("일", "", regex=False)
    )
    return s.where(s.str.fullmatch(r"\d{8}", na=False), "")


def iter_list(
    api_key: str,
    bgn_de: str,
    end_de: str,
    page_count: int = 100,
    timeout: int = 60,
    verify_ssl: bool = False,
    pblntf_ty: str = "B",
):
    page_no = 1
    while True:
        params = {
            "crtfc_key": api_key,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": pblntf_ty,
            "page_no": str(page_no),
            "page_count": str(page_count),
        }
        resp = requests.get(LIST_API_URL, params=params, timeout=timeout, verify=verify_ssl)
        resp.raise_for_status()
        data = resp.json()

        if str(data.get("status")) != "000":
            raise RuntimeError(f"[{data.get('status')}] {data.get('message')}")

        for item in data.get("list") or []:
            yield item

        total_page = int(data.get("total_page") or 0)
        if total_page == 0 or page_no >= total_page:
            break
        page_no += 1


def get_major_report_list(api_key: str, bgn_de: str, end_de: str, verify_ssl: bool = False) -> pd.DataFrame:
    rows = list(iter_list(api_key, bgn_de, end_de, verify_ssl=verify_ssl, pblntf_ty="B"))
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["report_nm_norm"] = df["report_nm"].apply(normalize_report_nm)

    out = df[df["report_nm_norm"].isin(TARGET_REPORT_TITLES)].copy()
    out["URL"] = out["rcept_no"].astype(str).apply(
        lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
    )
    return out.drop(columns=["report_nm_norm"]).reset_index(drop=True)


def _fetch_decision_list_by_corp_codes(
    api_key: str,
    corp_codes: list[str],
    api_url: str,
    bgn_de: str,
    end_de: str,
    sleep_sec: float = 0.05,
    timeout: int = 30,
) -> pd.DataFrame:
    chunks: list[pd.DataFrame] = []

    for corp_code in corp_codes:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
        }
        try:
            resp = requests.get(api_url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            continue

        if str(data.get("status")) != "000":
            time.sleep(sleep_sec)
            continue

        item_list = data.get("list") or []
        if not item_list:
            time.sleep(sleep_sec)
            continue

        df_api = pd.DataFrame(item_list)
        if df_api.empty:
            time.sleep(sleep_sec)
            continue

        if "corp_code" not in df_api.columns:
            df_api["corp_code"] = corp_code

        chunks.append(df_api)
        time.sleep(sleep_sec)

    if not chunks:
        return pd.DataFrame()

    return pd.concat(chunks, axis=0, ignore_index=True)


def _filter_by_rcept_window(df: pd.DataFrame, bgn_de: str, end_de: str) -> pd.DataFrame:
    if df.empty or "rcept_no" not in df.columns:
        return df.copy()

    out = df.copy()
    out["rcept_ymd"] = out["rcept_no"].astype(str).str[:8]
    out = out[
        out["rcept_ymd"].str.fullmatch(r"\d{8}", na=False)
        & (out["rcept_ymd"] >= bgn_de)
        & (out["rcept_ymd"] <= end_de)
    ].copy()
    out.drop(columns=["rcept_ymd"], inplace=True)
    return out


def _merge_target_metadata(source_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty or target_df.empty:
        return source_df.copy()

    out = source_df.copy()
    meta_cols = [c for c in ["corp_code", "corp_name", "rcept_no", "report_nm", "URL"] if c in target_df.columns]
    meta_df = target_df[meta_cols].drop_duplicates()

    merge_keys = [k for k in ["corp_code", "rcept_no"] if k in out.columns and k in meta_df.columns]
    if merge_keys:
        out = out.merge(meta_df, on=merge_keys, how="left")
    elif "corp_code" in out.columns and "corp_code" in meta_df.columns:
        out = out.merge(meta_df, on="corp_code", how="left")

    dedup_keys = [c for c in ["corp_code", "rcept_no"] if c in out.columns]
    return out.drop_duplicates(subset=dedup_keys).reset_index(drop=True) if dedup_keys else out.reset_index(drop=True)


def _coalesce_columns(df: pd.DataFrame, canonical: str, candidates: list[str]) -> pd.DataFrame:
    out = df.copy()
    if canonical in out.columns:
        return out

    present = [col for col in candidates if col in out.columns]
    if not present:
        return out

    out[canonical] = out[present[0]]
    for col in present[1:]:
        out[canonical] = out[canonical].where(out[canonical].notna(), out[col])
    return out


def _select_and_rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    fallback_map = {
        "corp_name": ["corp_name_x", "corp_name_y"],
        "corp_code": ["corp_code_x", "corp_code_y"],
        "rcept_no": ["rcept_no_x", "rcept_no_y"],
    }
    for canonical, candidates in fallback_map.items():
        out = _coalesce_columns(out, canonical, candidates)

    selected = pd.DataFrame(index=out.index)
    for source_col, renamed_col in mapping.items():
        selected[renamed_col] = out[source_col] if source_col in out.columns else ""
    return selected


def fetch_paid_increase_decision_df(
    api_key: str,
    major_list_df: pd.DataFrame,
    bgn_de: str,
    end_de: str,
    sleep_sec: float = 0.05,
    timeout: int = 30,
) -> pd.DataFrame:
    bgn_for_api = (datetime.strptime(bgn_de, "%Y%m%d") - relativedelta(months=8)).strftime("%Y%m%d")

    target = major_list_df[
        major_list_df["report_nm"].apply(normalize_report_nm).eq("주요사항보고서(유상증자결정)")
    ].copy()
    if target.empty:
        return pd.DataFrame()

    corp_codes = target["corp_code"].dropna().astype(str).unique().tolist()
    out = _fetch_decision_list_by_corp_codes(
        api_key=api_key,
        corp_codes=corp_codes,
        api_url=PIIC_API_URL,
        bgn_de=bgn_for_api,
        end_de=end_de,
        sleep_sec=sleep_sec,
        timeout=timeout,
    )
    if out.empty:
        return out

    out = _merge_target_metadata(out, target)
    return _filter_by_rcept_window(out, bgn_de, end_de)


def fetch_cb_bw_unified_df(
    api_key: str,
    major_list_df: pd.DataFrame,
    bgn_de: str,
    end_de: str,
    sleep_sec: float = 0.05,
    timeout: int = 30,
) -> pd.DataFrame:
    bgn_for_api = (datetime.strptime(bgn_de, "%Y%m%d") - relativedelta(months=8)).strftime("%Y%m%d")

    cb_target = major_list_df[
        major_list_df["report_nm"].apply(normalize_report_nm).eq("주요사항보고서(전환사채권발행결정)")
    ].copy()
    bw_target = major_list_df[
        major_list_df["report_nm"].apply(normalize_report_nm).eq("주요사항보고서(신주인수권부사채발행결정)")
    ].copy()

    cb_codes = cb_target["corp_code"].dropna().astype(str).unique().tolist()
    bw_codes = bw_target["corp_code"].dropna().astype(str).unique().tolist()

    cb_df = _fetch_decision_list_by_corp_codes(
        api_key=api_key,
        corp_codes=cb_codes,
        api_url=CB_API_URL,
        bgn_de=bgn_for_api,
        end_de=end_de,
        sleep_sec=sleep_sec,
        timeout=timeout,
    )
    bw_df = _fetch_decision_list_by_corp_codes(
        api_key=api_key,
        corp_codes=bw_codes,
        api_url=BW_API_URL,
        bgn_de=bgn_for_api,
        end_de=end_de,
        sleep_sec=sleep_sec,
        timeout=timeout,
    )

    cb_df = _filter_by_rcept_window(_merge_target_metadata(cb_df, cb_target), bgn_de, end_de)
    bw_df = _filter_by_rcept_window(_merge_target_metadata(bw_df, bw_target), bgn_de, end_de)

    cb_map = {
        "rcept_no": "접수번호",
        "corp_cls": "법인구분",
        "corp_code": "고유번호",
        "corp_name": "회사명",
        "bd_knd": "사채종류",
        "bd_fta": "사채총액",
        "sbd": "청약일",
        "pymd": "납입일",
        "bd_mtd": "만기일",
        "cv_rt": "전환비율",
        "cv_prc": "전환가액",
        "cvrqpd_bgd": "전환청구시작일",
    }
    bw_map = {
        "rcept_no": "접수번호",
        "corp_cls": "법인구분",
        "corp_code": "고유번호",
        "corp_name": "회사명",
        "bd_knd": "사채종류",
        "bd_fta": "사채총액",
        "sbd": "청약일",
        "pymd": "납입일",
        "bd_mtd": "만기일",
        "ex_rt": "전환비율",
        "ex_prc": "전환가액",
        "expd_bgd": "전환청구시작일",
    }

    cb_out = _select_and_rename_columns(cb_df, cb_map) if not cb_df.empty else pd.DataFrame(columns=list(cb_map.values()))
    bw_out = _select_and_rename_columns(bw_df, bw_map) if not bw_df.empty else pd.DataFrame(columns=list(bw_map.values()))

    out = pd.concat([cb_out, bw_out], ignore_index=True)
    if out.empty:
        return out

    for col in ["사채총액", "전환가액"]:
        if col in out.columns:
            out[col] = normalize_numeric_series(out[col])

    for col in ["청약일", "납입일", "만기일", "전환청구시작일"]:
        if col in out.columns:
            out[col] = normalize_date_series(out[col])

    out["대표이사"] = ""
    out["주소"] = ""
    out["작성책임자_직책"] = ""
    out["작성책임자_성명"] = ""
    out["작성책임자_전화번호"] = ""
    out["URL"] = out["접수번호"].astype(str).apply(
        lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
    )

    out = out.drop_duplicates(subset=["접수번호", "고유번호"]).reset_index(drop=True)
    return out


def add_finance_columns(piic_df: pd.DataFrame) -> pd.DataFrame:
    if piic_df.empty:
        return piic_df.copy()

    df = piic_df.copy()

    fdpp_cols = [c for c in df.columns if "fdpp" in c.lower()]
    df["fdpp_sum"] = df[fdpp_cols].apply(to_numeric_series).sum(axis=1) if fdpp_cols else 0

    nstk_cols = [c for c in ["nstk_ostk_cnt", "nstk_estk_cnt"] if c in df.columns]
    df["nstk_sum"] = df[nstk_cols].apply(to_numeric_series).sum(axis=1) if nstk_cols else 0

    df["nstk_ps"] = (
        (df["fdpp_sum"] / df["nstk_sum"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        .round()
        .astype(int)
    )
    return df


def fetch_report_fulltext_df(api_key: str, df_with_rcept_no: pd.DataFrame, timeout: int = 60) -> pd.DataFrame:
    rows = []
    rcept_nos = df_with_rcept_no["rcept_no"].dropna().astype(str).unique().tolist()

    for rcept_no in rcept_nos:
        try:
            res = requests.get(
                DOC_API_URL,
                params={"crtfc_key": api_key, "rcept_no": rcept_no},
                timeout=timeout,
            )
            res.raise_for_status()

            zf = zipfile.ZipFile(io.BytesIO(res.content))
            xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not xml_names:
                rows.append({"rcept_no": rcept_no, "fulltext_xml": None, "error": "xml file not found"})
                continue

            xml_text = zf.read(xml_names[0]).decode("utf-8", errors="ignore")
            rows.append({"rcept_no": rcept_no, "fulltext_xml": xml_text})
        except Exception as exc:
            rows.append({"rcept_no": rcept_no, "fulltext_xml": None, "error": str(exc)})

    return pd.DataFrame(rows)


def parse_contact_fields(xml_text: str) -> dict:
    raw_cells = re.findall(r"<TD[^>]*>(.*?)</TD>", xml_text, flags=re.IGNORECASE | re.DOTALL)

    def clean(s: str) -> str:
        s = re.sub(r"<[^>]+>", "", s)
        s = html.unescape(s).replace("\xa0", " ")
        return re.sub(r"\s+", " ", s).strip()

    cells = [clean(x) for x in raw_cells]
    norm = [re.sub(r"[\s:()]", "", c) for c in cells]

    def next_non_empty(idx: int):
        for j in range(idx + 1, len(cells)):
            if cells[j]:
                return cells[j]
        return None

    def strip_prefix(text: str, pattern: str):
        return re.sub(pattern, "", text).strip()

    out = {
        "대표이사": None,
        "본점소재지": None,
        "작성책임자_직책": None,
        "작성책임자_성명": None,
        "작성책임자_전화번호": None,
    }

    writer_start = None
    for i, n_val in enumerate(norm):
        if "대표이사" in n_val and out["대표이사"] is None:
            out["대표이사"] = next_non_empty(i)
        elif "본점소재지" in n_val and out["본점소재지"] is None:
            out["본점소재지"] = next_non_empty(i)
        elif "작성책임자" in n_val and writer_start is None:
            writer_start = i

    if writer_start is not None:
        for j in range(writer_start + 1, min(writer_start + 12, len(cells))):
            t = cells[j]
            n_val = norm[j]
            if "직책" in n_val and out["작성책임자_직책"] is None:
                out["작성책임자_직책"] = strip_prefix(t, r"^\(?\s*직\s*책\s*\)?\s*")
            elif "성명" in n_val and out["작성책임자_성명"] is None:
                out["작성책임자_성명"] = strip_prefix(t, r"^\(?\s*성\s*명\s*\)?\s*")
            elif "전화" in n_val and out["작성책임자_전화번호"] is None:
                out["작성책임자_전화번호"] = strip_prefix(t, r"^\(?\s*전\s*화\s*\)?\s*")

    return out


def parse_schedule_fields(xml_text: str) -> dict:
    out = {"납입일": None, "신주상장예정일": None}
    if not isinstance(xml_text, str) or not xml_text:
        return out

    m_pym = re.search(r'AUNIT\s*=\s*"PYM_DT"[^>]*AUNITVALUE\s*=\s*"([^"]+)"', xml_text, flags=re.IGNORECASE)
    m_lst = re.search(r'AUNIT\s*=\s*"LST_PLN_DT"[^>]*AUNITVALUE\s*=\s*"([^"]+)"', xml_text, flags=re.IGNORECASE)

    if m_pym:
        v = m_pym.group(1).strip()
        out["납입일"] = None if v in {"", "-"} else v
    if m_lst:
        v = m_lst.group(1).strip()
        out["신주상장예정일"] = None if v in {"", "-"} else v

    if out["납입일"] is None:
        m = re.search(r'납입일</TD>\s*<T[UE][^>]*>(.*?)</T[UE]>', xml_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            txt = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            out["납입일"] = None if txt in {"", "-"} else txt

    if out["신주상장예정일"] is None:
        m = re.search(r'신주의\s*상장\s*예정일</TD>\s*<T[UE][^>]*>(.*?)</T[UE]>', xml_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            txt = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            out["신주상장예정일"] = None if txt in {"", "-"} else txt

    return out


def build_output_df(piic_df: pd.DataFrame, fulltext_df: pd.DataFrame) -> pd.DataFrame:
    if piic_df.empty:
        return pd.DataFrame()

    name_col = "corp_name" if "corp_name" in piic_df.columns else "corp_name_x"
    keep_cols = [c for c in ["rcept_no", name_col, "ic_mthn", "fdpp_sum", "nstk_ps", "nstk_sum", "URL"] if c in piic_df.columns]
    out = piic_df[keep_cols].copy()

    if name_col != "corp_name":
        out = out.rename(columns={name_col: "corp_name"})

    parsed_rows = []
    for _, row in fulltext_df.iterrows():
        xml_text = row.get("fulltext_xml")
        base = {
            "대표이사": None,
            "본점소재지": None,
            "작성책임자_직책": None,
            "작성책임자_성명": None,
            "작성책임자_전화번호": None,
            "납입일": None,
            "신주상장예정일": None,
        }
        if isinstance(xml_text, str) and xml_text:
            base.update(parse_contact_fields(xml_text))
            base.update(parse_schedule_fields(xml_text))
        base["rcept_no"] = row.get("rcept_no")
        parsed_rows.append(base)

    parse_df = pd.DataFrame(parsed_rows).drop_duplicates(subset=["rcept_no"])
    return out.merge(parse_df, on="rcept_no", how="left")


def fill_contact_fields_from_fulltext(target_df: pd.DataFrame, fulltext_df: pd.DataFrame) -> pd.DataFrame:
    if target_df.empty:
        return target_df.copy()

    base_cols = ["대표이사", "주소", "작성책임자_직책", "작성책임자_성명", "작성책임자_전화번호"]

    parsed_rows = []
    for _, row in fulltext_df.iterrows():
        xml_text = row.get("fulltext_xml")
        parsed = parse_contact_fields(xml_text) if isinstance(xml_text, str) and xml_text else {}
        parsed_rows.append(
            {
                "rcept_no": row.get("rcept_no"),
                "대표이사": parsed.get("대표이사"),
                "주소": parsed.get("본점소재지"),
                "작성책임자_직책": parsed.get("작성책임자_직책"),
                "작성책임자_성명": parsed.get("작성책임자_성명"),
                "작성책임자_전화번호": parsed.get("작성책임자_전화번호"),
            }
        )

    parsed_df = pd.DataFrame(parsed_rows).drop_duplicates(subset=["rcept_no"])
    out = target_df.copy()

    if "rcept_no" not in out.columns:
        raise ValueError("target_df에 rcept_no 컬럼이 필요합니다.")

    for col in base_cols:
        if col not in out.columns:
            out[col] = ""

    out = out.merge(parsed_df, on="rcept_no", how="left", suffixes=("", "_parsed"))

    for col in base_cols:
        parsed_col = f"{col}_parsed"
        if parsed_col in out.columns:
            out[col] = out[col].fillna("")
            out[parsed_col] = out[parsed_col].fillna("")
            out[col] = out[col].where(out[col].astype(str).str.strip() != "", out[parsed_col])
            out.drop(columns=[parsed_col], inplace=True)

    return out


def format_output_df(output_df: pd.DataFrame) -> pd.DataFrame:
    if output_df.empty:
        return output_df.copy()

    df = output_df.rename(columns=OUT_RENAME_MAP).copy()
    for col in OUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df.loc[:, OUT_COLUMNS]


def format_cb_bw_output_df(output_df: pd.DataFrame) -> pd.DataFrame:
    if output_df.empty:
        return output_df.copy()

    df = output_df.copy()
    for col in CB_BW_OUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df.loc[:, CB_BW_OUT_COLUMNS]


def _prepare_major_paid_increase_sheet(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["대표이사", "작성책임자_직책", "작성책임자_성명"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace(r"\s+", "", regex=True)
            out.loc[out[col].isin(["None", "nan"]), col] = ""

    for col in ["발행주식수", "발행가액", "발행금액"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace(",", "", regex=False).replace("-", pd.NA)
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


def _prepare_cb_bw_sheet(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["대표이사", "작성책임자_직책", "작성책임자_성명"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace(r"\s+", "", regex=True)
            out.loc[out[col].isin(["None", "nan"]), col] = ""

    for col in ["사채총액", "전환가액"]:
        if col in out.columns:
            out[col] = normalize_numeric_series(out[col])

    for col in ["청약일", "납입일", "만기일", "전환청구시작일"]:
        if col in out.columns:
            out[col] = normalize_date_series(out[col])

    return out


def _write_sheet(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str, num_cols: list[str]):
    df.to_excel(writer, index=False, sheet_name=sheet_name)
    ws = writer.sheets[sheet_name]
    wb = writer.book
    comma_fmt = wb.add_format({"num_format": "#,##0"})

    for col in num_cols:
        if col in df.columns:
            idx = df.columns.get_loc(col)
            ws.set_column(idx, idx, 14, comma_fmt)

    if "URL" in df.columns:
        idx = df.columns.get_loc("URL")
        ws.set_column(idx, idx, 53)


def _write_major_excel(file_obj, paid_increase_df: pd.DataFrame, cb_bw_df: pd.DataFrame):
    df_paid = _prepare_major_paid_increase_sheet(paid_increase_df)
    df_cb_bw = _prepare_cb_bw_sheet(cb_bw_df)

    with pd.ExcelWriter(file_obj, engine="xlsxwriter") as writer:
        _write_sheet(writer, df_paid, "제3자배정_유상증자", ["발행주식수", "발행가액", "발행금액"])
        _write_sheet(writer, df_cb_bw, "전환사채_신주인수권부사채", ["사채총액", "전환가액"])


def run_major_paid_increase_report_bytes(
    api_key: str,
    bgn_de: str,
    end_de: str,
    list_timeout: int = 60,
    request_timeout: int = 30,
    sleep_sec: float = 0.05,
    verify_ssl: bool = False,
) -> tuple[bytes, str] | None:
    major_list_df = get_major_report_list(api_key, bgn_de, end_de, verify_ssl=verify_ssl)
    if major_list_df.empty:
        return None

    piic_df = fetch_paid_increase_decision_df(
        api_key=api_key,
        major_list_df=major_list_df,
        bgn_de=bgn_de,
        end_de=end_de,
        sleep_sec=sleep_sec,
        timeout=request_timeout,
    )
    output_df = pd.DataFrame(columns=OUT_COLUMNS)
    if not piic_df.empty:
        piic_df = add_finance_columns(piic_df)
        fulltext_df = fetch_report_fulltext_df(api_key, piic_df, timeout=list_timeout)
        output_df = build_output_df(piic_df, fulltext_df)
        output_df = format_output_df(output_df)

    cb_bw_df = fetch_cb_bw_unified_df(
        api_key=api_key,
        major_list_df=major_list_df,
        bgn_de=bgn_de,
        end_de=end_de,
        sleep_sec=sleep_sec,
        timeout=request_timeout,
    )
    cb_bw_output_df = pd.DataFrame(columns=CB_BW_OUT_COLUMNS)
    if not cb_bw_df.empty:
        fulltext_cb_bw = fetch_report_fulltext_df(
            api_key,
            cb_bw_df.rename(columns={"접수번호": "rcept_no"}),
            timeout=list_timeout,
        )
        cb_bw_output_df = fill_contact_fields_from_fulltext(
            cb_bw_df.rename(columns={"접수번호": "rcept_no"}),
            fulltext_cb_bw,
        )
        cb_bw_output_df = cb_bw_output_df.rename(columns={"rcept_no": "접수번호"})
        cb_bw_output_df = format_cb_bw_output_df(cb_bw_output_df)

    if output_df.empty and cb_bw_output_df.empty:
        return None

    kst_now = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M")
    filename = f"DART_주요사항보고서_주식연계채권등_F{bgn_de}_T{end_de}_추출시간_{kst_now}.xlsx"

    buffer = BytesIO()
    _write_major_excel(buffer, output_df, cb_bw_output_df)
    return buffer.getvalue(), filename
