from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo
import time
import warnings

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from xlsxwriter.utility import xl_col_to_name


warnings.filterwarnings("ignore", message="Unverified HTTPS request")

REPORT_NAME_DEFAULT = "증권신고서(지분증권)"
LIST_API_URL = "https://opendart.fss.or.kr/api/list.json"
ESTK_API_URL = "https://opendart.fss.or.kr/api/estkRs.json"
COMPANY_API_URL = "https://opendart.fss.or.kr/api/company.json"

MAP_DICT = {
    "corp_cls": "상장구분",
    "corp_code": "고유번호",
    "corp_name": "회사명",
    "stock_code": "주식코드",
    "stock_name": "주식명",
    "rcept_no": "접수번호",
    "report_nm": "보고서명",
    "sbd": "청약기일",
    "pymd": "납입기일",
    "stkcnt": "증권수량",
    "slprc": "모집(매출)가액",
    "slta": "모집(매출)총액",
    "bizr_no": "사업자등록번호",
    "ceo_nm": "대표자명",
    "adres": "주소",
    "phn_no": "전화번호",
}

CORP_CLS_MAP = {"Y": "코스피", "K": "코스닥", "N": "코넥스", "E": "기타"}


def normalize_corp_code(value) -> str:
    if pd.isna(value):
        return ""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return digits.zfill(8) if digits else ""


def format_bizr_no(value) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
    return text


def _get_json(url: str, params: dict[str, str], timeout: int, verify_ssl: bool) -> dict:
    resp = requests.get(url, params=params, timeout=timeout, verify=verify_ssl)
    resp.raise_for_status()
    return resp.json()


def iter_list(
    api_key: str,
    bgn_de: str,
    end_de: str,
    page_count: int = 100,
    timeout: int = 60,
    verify_ssl: bool = False,
):
    page_no = 1

    while True:
        params = {
            "crtfc_key": api_key,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": str(page_no),
            "page_count": str(page_count),
        }
        data = _get_json(LIST_API_URL, params=params, timeout=timeout, verify_ssl=verify_ssl)

        if str(data.get("status")) != "000":
            raise RuntimeError(data.get("message", "DART error"))

        items = data.get("list") or []
        for item in items:
            yield item

        total_page = int(data.get("total_page") or 0)
        if total_page == 0 or page_no >= total_page:
            break
        page_no += 1


def _build_report_df(
    api_key: str,
    bgn_de: str,
    end_de: str,
    report_name: str,
    report_filter_text: str | None,
    page_count: int,
    timeout: int,
    verify_ssl: bool,
) -> pd.DataFrame:
    filtered = [
        item
        for item in iter_list(
            api_key=api_key,
            bgn_de=bgn_de,
            end_de=end_de,
            page_count=page_count,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
        if report_name in (item.get("report_nm") or "")
    ]

    report_df = pd.DataFrame(filtered)
    if report_df.empty:
        return report_df

    if report_filter_text:
        report_df = report_df.loc[report_df["report_nm"].str.contains(report_filter_text, na=False)].copy()

    if report_df.empty:
        return report_df

    report_df["corp_code"] = report_df["corp_code"].apply(normalize_corp_code)
    report_df["rcept_no"] = report_df["rcept_no"].astype(str)
    report_df["URL"] = report_df["rcept_no"].apply(
        lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
    )
    return report_df.reset_index(drop=True)


def _build_check_list(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["회사명", "보고서명", "접수일", "접수번호", "URL"])

    correction_mask = report_df["report_nm"].str.contains(r"\[.*정정.*\]|\[발행조건확정\]", na=False)
    check_list = report_df.loc[
        correction_mask,
        ["corp_code", "corp_name", "report_nm", "rcept_dt", "rcept_no", "URL"],
    ].copy()

    if check_list.empty:
        return pd.DataFrame(columns=["회사명", "보고서명", "접수일", "접수번호", "URL"])

    check_list["corp_code"] = check_list["corp_code"].apply(normalize_corp_code)
    check_list["has_final_terms"] = check_list["report_nm"].str.contains(r"\[발행조건확정\]", na=False)
    check_list = check_list.sort_values(
        by=["corp_code", "corp_name", "has_final_terms", "rcept_dt", "rcept_no"],
        ascending=[True, True, False, False, False],
        kind="mergesort",
    )
    check_list = (
        check_list.groupby(["corp_code", "corp_name"], as_index=False, sort=False)
        .head(1)
        .reset_index(drop=True)
    )

    check_list = check_list.rename(
        columns={
            "corp_name": "회사명",
            "report_nm": "보고서명",
            "rcept_dt": "접수일",
            "rcept_no": "접수번호",
        }
    )
    return check_list.loc[:, ["회사명", "보고서명", "접수일", "접수번호", "URL"]]


def _pick_preferred_report_meta(meta_df: pd.DataFrame) -> pd.DataFrame:
    if meta_df.empty:
        return meta_df

    out = meta_df.copy()
    out["rcept_no"] = out["rcept_no"].astype(str)
    out["has_final_terms"] = out["report_nm"].str.contains(r"\[발행조건확정\]", na=False)
    out = out.sort_values(
        by=["has_final_terms", "rcept_dt", "rcept_no"],
        ascending=[False, False, False],
        kind="mergesort",
    )
    return out.head(1).drop(columns=["has_final_terms"]).reset_index(drop=True)


def get_company_overview_fields(
    api_key: str,
    corp_code: str,
    timeout: int = 30,
    verify_ssl: bool = False,
) -> dict[str, str]:
    normalized = normalize_corp_code(corp_code)
    params = {
        "crtfc_key": api_key,
        "corp_code": normalized,
    }

    try:
        data = _get_json(COMPANY_API_URL, params=params, timeout=timeout, verify_ssl=verify_ssl)
    except Exception:
        data = {}

    if str(data.get("status")) != "000":
        return {
            "corp_code": normalized,
            "bizr_no": "",
            "ceo_nm": "",
            "adres": "",
            "phn_no": "",
        }

    return {
        "corp_code": normalized,
        "bizr_no": data.get("bizr_no", ""),
        "ceo_nm": data.get("ceo_nm", ""),
        "adres": data.get("adres", ""),
        "phn_no": data.get("phn_no", ""),
    }


def _fetch_overview_df(
    api_key: str,
    corp_codes: list[str],
    timeout: int,
    sleep_sec: float,
    verify_ssl: bool,
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    for corp_code in corp_codes:
        rows.append(
            get_company_overview_fields(
                api_key=api_key,
                corp_code=corp_code,
                timeout=timeout,
                verify_ssl=verify_ssl,
            )
        )
        time.sleep(sleep_sec)

    if not rows:
        return pd.DataFrame(columns=["corp_code", "bizr_no", "ceo_nm", "adres", "phn_no"])

    out = pd.DataFrame(rows)
    out["corp_code"] = out["corp_code"].apply(normalize_corp_code)
    return out.drop_duplicates(subset=["corp_code"]).reset_index(drop=True)


def _first_non_empty(series: pd.Series):
    for value in series:
        if pd.isna(value):
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none"}:
            return value
    return ""


def merge_estk_detail_columns(df_base: pd.DataFrame, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = df_base.copy()
    key_candidates = ["rcept_no", "corp_code", "corp_name", "stock_code", "stock_name"]
    wanted_cols = ["stkcnt", "slprc", "slta"]

    if "corp_code" in out.columns:
        out["corp_code"] = out["corp_code"].apply(normalize_corp_code)
    if "rcept_no" in out.columns:
        out["rcept_no"] = out["rcept_no"].astype(str)

    for df_part in dfs.values():
        if df_part is None or df_part.empty:
            continue

        part = df_part.copy()
        if "corp_code" in part.columns:
            part["corp_code"] = part["corp_code"].apply(normalize_corp_code)
        if "rcept_no" in part.columns:
            part["rcept_no"] = part["rcept_no"].astype(str)

        value_cols = [col for col in wanted_cols if col in part.columns]
        if not value_cols:
            continue

        merge_keys = [col for col in key_candidates if col in out.columns and col in part.columns]
        if not merge_keys:
            continue

        grouped = (
            part.loc[:, merge_keys + value_cols]
            .groupby(merge_keys, as_index=False)
            .agg({col: _first_non_empty for col in value_cols})
        )

        out = out.merge(grouped, on=merge_keys, how="left", suffixes=("", "_detail"))

        for col in value_cols:
            detail_col = f"{col}_detail"
            if detail_col not in out.columns:
                continue

            if col in out.columns:
                left = out[col].fillna("").astype(str).str.strip()
                right = out[detail_col].fillna("").astype(str)
                out[col] = out[col].where(left != "", right)
            else:
                out[col] = out[detail_col]

            out = out.drop(columns=[detail_col])

    return out


def merge_company_overview(df_base: pd.DataFrame, overview_df: pd.DataFrame) -> pd.DataFrame:
    out = df_base.copy()

    if "corp_code" in out.columns:
        out["corp_code"] = out["corp_code"].apply(normalize_corp_code)

    overview = overview_df.copy()
    if "corp_code" in overview.columns:
        overview["corp_code"] = overview["corp_code"].apply(normalize_corp_code)

    out = out.merge(overview, on="corp_code", how="left", suffixes=("", "_overview"))

    for col in ["bizr_no", "ceo_nm", "adres", "phn_no"]:
        overview_col = f"{col}_overview"
        if overview_col in out.columns:
            if col in out.columns:
                left = out[col].fillna("").astype(str).str.strip()
                right = out[overview_col].fillna("").astype(str)
                out[col] = out[col].where(left != "", right)
            else:
                out[col] = out[overview_col]
            out = out.drop(columns=[overview_col])

        if col not in out.columns:
            out[col] = ""

    out["bizr_no"] = out["bizr_no"].apply(format_bizr_no)
    return out


def _fetch_estk_groups(
    api_key: str,
    corp_code: str,
    bgn_de: str,
    end_de: str,
    timeout: int,
    verify_ssl: bool,
) -> dict[str, pd.DataFrame]:
    params = {
        "crtfc_key": api_key,
        "corp_code": normalize_corp_code(corp_code),
        "bgn_de": bgn_de,
        "end_de": end_de,
    }
    data = _get_json(ESTK_API_URL, params=params, timeout=timeout, verify_ssl=verify_ssl)

    if str(data.get("status")) != "000":
        raise RuntimeError(f"{data.get('status')}: {data.get('message')}")

    dfs: dict[str, pd.DataFrame] = {}
    for group in data.get("group", []):
        title = group.get("title", "group")
        dfs[title] = pd.DataFrame(group.get("list", []))
    return dfs


def _build_general_sheet_df(
    api_key: str,
    report_df: pd.DataFrame,
    bgn_de: str,
    end_de: str,
    request_timeout: int,
    sleep_sec: float,
    verify_ssl: bool,
) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame()

    corp_codes = report_df["corp_code"].dropna().astype(str).unique().tolist()
    overview_df = _fetch_overview_df(
        api_key=api_key,
        corp_codes=corp_codes,
        timeout=request_timeout,
        sleep_sec=sleep_sec,
        verify_ssl=verify_ssl,
    )

    base_list: list[pd.DataFrame] = []
    api_bgn_de = (datetime.strptime(bgn_de, "%Y%m%d") - relativedelta(months=6)).strftime("%Y%m%d")
    api_end_de = end_de

    for corp_code in corp_codes:
        target_meta = report_df.loc[
            report_df["corp_code"] == normalize_corp_code(corp_code),
            ["rcept_no", "corp_name", "corp_cls", "report_nm", "rcept_dt", "URL"],
        ].drop_duplicates()
        preferred_meta = _pick_preferred_report_meta(target_meta)

        try:
            dfs = _fetch_estk_groups(
                api_key=api_key,
                corp_code=corp_code,
                bgn_de=api_bgn_de,
                end_de=api_end_de,
                timeout=request_timeout,
                verify_ssl=verify_ssl,
            )
        except Exception as exc:
            print(f"[SKIP] estkRs 실패: {corp_code} / {exc}")
            time.sleep(sleep_sec)
            continue

        df_base = dfs.get("일반사항")
        if df_base is None or df_base.empty:
            print(f"[SKIP] 일반사항 없음: {corp_code}")
            time.sleep(sleep_sec)
            continue

        df_base = df_base.copy()
        if "corp_code" not in df_base.columns:
            df_base["corp_code"] = normalize_corp_code(corp_code)

        df_base = merge_estk_detail_columns(df_base, dfs)
        df_base = merge_company_overview(df_base, overview_df)

        if "rcept_no" in df_base.columns:
            df_base["rcept_no"] = df_base["rcept_no"].astype(str)
            if not preferred_meta.empty:
                preferred_rcept_no = preferred_meta.loc[0, "rcept_no"]
                if preferred_rcept_no in df_base["rcept_no"].values:
                    df_base = df_base.loc[df_base["rcept_no"] == preferred_rcept_no].copy()
                else:
                    print(f"[INFO] preferred rcept_no fallback: {corp_code} / {preferred_rcept_no}")

        if not preferred_meta.empty:
            preferred_row = preferred_meta.iloc[0]
            df_base["corp_name"] = preferred_row["corp_name"]
            df_base["corp_cls"] = preferred_row["corp_cls"]
            df_base["URL"] = preferred_row["URL"]
        elif "URL" not in df_base.columns:
            df_base["URL"] = df_base["rcept_no"].astype(str).apply(
                lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
            )

        df_base = df_base.rename(columns=MAP_DICT)
        if "사업자등록번호" not in df_base.columns:
            df_base["사업자등록번호"] = ""
        else:
            df_base["사업자등록번호"] = df_base["사업자등록번호"].apply(format_bizr_no)

        base_list.append(df_base)
        time.sleep(sleep_sec)

    if not base_list:
        return pd.DataFrame()

    df_base = pd.concat(base_list, ignore_index=True).drop_duplicates().reset_index(drop=True)

    sort_cols = [
        "회사명",
        "사업자등록번호",
        "상장구분",
        "증권수량",
        "모집(매출)가액",
        "모집(매출)총액",
        "청약기일",
        "납입기일",
        "발행회사 담당",
        "실무담당",
        "당사 연락처(비고)",
        "URL",
    ]

    df_base["발행회사 담당"] = ""
    df_base["실무담당"] = ""
    df_base["당사 연락처(비고)"] = ""

    for col in sort_cols:
        if col not in df_base.columns:
            df_base[col] = ""

    df_base = df_base.loc[:, sort_cols]

    if "상장구분" in df_base.columns:
        df_base["상장구분"] = df_base["상장구분"].map(CORP_CLS_MAP).fillna(df_base["상장구분"])

    if "납입기일" in df_base.columns:
        df_base = df_base.sort_values(by="납입기일", ascending=False, kind="mergesort")
    else:
        df_base = df_base.sort_values(by="회사명", ascending=True, kind="mergesort")

    return df_base.reset_index(drop=True)


def _write_excel(file_obj, check_list: pd.DataFrame, df_base: pd.DataFrame):
    with pd.ExcelWriter(file_obj, engine="xlsxwriter") as writer:
        check_list.to_excel(writer, index=False, sheet_name="검토목록")
        df_base.to_excel(writer, index=False, sheet_name="일반사항")

        workbook = writer.book
        highlight_fmt = workbook.add_format({"bg_color": "#F8D7DA"})

        ws_base = writer.sheets["일반사항"]
        ws_check = writer.sheets["검토목록"]

        for col_name, width in {"사업자등록번호": 16, "URL": 53}.items():
            if col_name in df_base.columns:
                idx = df_base.columns.get_loc(col_name)
                ws_base.set_column(idx, idx, width)

        if not check_list.empty and "회사명" in check_list.columns and "회사명" in df_base.columns:
            chk_col_idx = check_list.columns.get_loc("회사명")
            chk_col_letter = xl_col_to_name(chk_col_idx)
            chk_last_row = len(check_list) + 1
            chk_range = f"'검토목록'!${chk_col_letter}$2:${chk_col_letter}${chk_last_row}"

            name_col_idx = df_base.columns.get_loc("회사명")
            name_col_letter = xl_col_to_name(name_col_idx)
            last_row = len(df_base)
            last_col = len(df_base.columns) - 1

            if last_row >= 1:
                formula = f'=ISNUMBER(MATCH(${name_col_letter}2,{chk_range},0))'
                ws_base.conditional_format(
                    1,
                    0,
                    last_row,
                    last_col,
                    {
                        "type": "formula",
                        "criteria": formula,
                        "format": highlight_fmt,
                    },
                )

        ws_base.activate()
        ws_check.hide()


def run_rights_issue_report(
    api_key: str,
    bgn_de: str,
    end_de: str,
    out_dir: str | Path = "results",
    report_name: str = REPORT_NAME_DEFAULT,
    report_filter_text: str | None = None,
    page_count: int = 100,
    list_timeout: int = 60,
    request_timeout: int = 30,
    sleep_sec: float = 0.05,
    verify_ssl: bool = False,
) -> str | None:
    report_df = _build_report_df(
        api_key=api_key,
        bgn_de=bgn_de,
        end_de=end_de,
        report_name=report_name,
        report_filter_text=report_filter_text,
        page_count=page_count,
        timeout=list_timeout,
        verify_ssl=verify_ssl,
    )
    if report_df.empty:
        return None

    check_list = _build_check_list(report_df)
    df_base = _build_general_sheet_df(
        api_key=api_key,
        report_df=report_df,
        bgn_de=bgn_de,
        end_de=end_de,
        request_timeout=request_timeout,
        sleep_sec=sleep_sec,
        verify_ssl=verify_ssl,
    )
    if df_base.empty:
        return None

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    kst_now = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M")
    out_path = out_dir / f"DART_증권신고서_지분증권_F{bgn_de}_T{end_de}_추출시간_{kst_now}.xlsx"

    _write_excel(out_path, check_list, df_base)
    return str(out_path)


def run_rights_issue_report_bytes(
    api_key: str,
    bgn_de: str,
    end_de: str,
    report_name: str = REPORT_NAME_DEFAULT,
    report_filter_text: str | None = None,
    page_count: int = 100,
    list_timeout: int = 60,
    request_timeout: int = 30,
    sleep_sec: float = 0.05,
    verify_ssl: bool = False,
) -> tuple[bytes, str] | None:
    report_df = _build_report_df(
        api_key=api_key,
        bgn_de=bgn_de,
        end_de=end_de,
        report_name=report_name,
        report_filter_text=report_filter_text,
        page_count=page_count,
        timeout=list_timeout,
        verify_ssl=verify_ssl,
    )
    if report_df.empty:
        return None

    check_list = _build_check_list(report_df)
    df_base = _build_general_sheet_df(
        api_key=api_key,
        report_df=report_df,
        bgn_de=bgn_de,
        end_de=end_de,
        request_timeout=request_timeout,
        sleep_sec=sleep_sec,
        verify_ssl=verify_ssl,
    )
    if df_base.empty:
        return None

    kst_now = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M")
    filename = f"DART_증권신고서_지분증권_F{bgn_de}_T{end_de}_추출시간_{kst_now}.xlsx"

    buffer = BytesIO()
    _write_excel(buffer, check_list, df_base)
    return buffer.getvalue(), filename
