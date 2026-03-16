from __future__ import annotations

from pathlib import Path
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from xlsxwriter.utility import xl_col_to_name
from zoneinfo import ZoneInfo

import time
import warnings
import requests
import pandas as pd


REPORT_NAME_DEFAULT = "증권신고서(지분증권)"

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
    "sband": "청약공고일",
    "asand": "배정공고일",
    "asstd": "배정기준일",
    "exstk": "행사대상증권",
    "exprc": "행사가격",
    "expd": "행사기간",
    "stksen": "증권의종류",
    "stkcnt": "증권수량",
    "fv": "액면가액",
    "slprc": "모집(매출)가액",
    "slta": "모집(매출)총액",
    "slmthn": "모집(매출)방법",
    "actsen": "인수인구분",
    "actnmn": "인수인명",
    "udtcnt": "인수수량",
    "udtamt": "인수금액",
    "udtprc": "인수대가",
    "udtmth": "인수방법",
    "se": "구분",
    "amt": "금액",
    "hdr": "보유자",
    "rl_cmp": "회사와의관계",
    "bfsl_hdstk": "매출전보유증권수",
    "slstk": "매출증권수",
    "atsl_hdstk": "매출후보유증권수",
    "grtrs": "부여사유",
    "exavivr": "행사가능투자자",
    "grtcnt": "부여수량",
}

CORP_CLS_MAP = {"Y": "코스피", "K": "코스닥", "N": "코넥스", "E": "기타"}


def iter_list(api_key: str, bgn_de: str, end_de: str, page_count: int = 100, timeout: int = 60, verify_ssl: bool = False):
    page_no = 1
    while True:
        params = {
            "crtfc_key": api_key,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": str(page_no),
            "page_count": str(page_count),
        }
        resp = requests.get(
            "https://opendart.fss.or.kr/api/list.json",
            params=params,
            timeout=timeout,
            verify=verify_ssl,
        )
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("status")) != "000":
            raise RuntimeError(data.get("message", "DART error"))

        items = data.get("list") or []
        for item in items:
            yield item

        total_page = int(data.get("total_page") or 0)
        if page_no >= total_page or total_page == 0:
            break
        page_no += 1


def get_company_overview_df(api_key: str, corp_code: str) -> pd.DataFrame:
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "000":
        raise RuntimeError(f"[{data.get('status')}] {data.get('message')}")

    df = pd.DataFrame([data])

    mapping_dict = {
        "corp_name": "회사명",
        "corp_code": "고유번호",
        "corp_name_eng": "영문명칭",
        "stock_name": "종목명/약식명칭",
        "stock_code": "종목코드",
        "ceo_nm": "대표자명",
        "corp_cls": "상장구분",
        "jurir_no": "법인등록번호",
        "bizr_no": "사업자등록번호",
        "adres": "주소",
        "hm_url": "홈페이지",
        "ir_url": "IR홈페이지",
        "phn_no": "전화번호",
        "fax_no": "팩스번호",
        "induty_code": "업종코드",
        "est_dt": "설립일(YYYYMMDD)",
        "acc_mt": "결산월(MM)",
    }

    df = df.rename(columns=mapping_dict)
    df = df.drop(columns=[c for c in ["status", "message"] if c in df.columns])
    return df


def get_company_overview_fields(api_key: str, corp_code: str) -> pd.DataFrame:
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "000":
        raise RuntimeError(f"[{data.get('status')}] {data.get('message')}")

    return pd.DataFrame(
        [
            {
                "corp_code": corp_code,
                "bizr_no": data.get("bizr_no", ""),
                "ceo_nm": data.get("ceo_nm", ""),
                "adres": data.get("adres", ""),
                "phn_no": data.get("phn_no", ""),
                "fax_no": data.get("fax_no", ""),
            }
        ]
    )


def get_empty_company_overview_fields(corp_code: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "corp_code": corp_code,
                "bizr_no": "",
                "ceo_nm": "",
                "adres": "",
                "phn_no": "",
                "fax_no": "",
            }
        ]
    )


def format_bizr_no(value) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
    return text


def normalize_corp_code(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return digits.zfill(8)
    return text


def merge_company_overview(df_base: pd.DataFrame, overview: pd.DataFrame) -> pd.DataFrame:
    out = df_base.copy()

    if "corp_code" in out.columns:
        out["corp_code"] = out["corp_code"].apply(normalize_corp_code)
    if "corp_code" in overview.columns:
        overview = overview.copy()
        overview["corp_code"] = overview["corp_code"].apply(normalize_corp_code)

    merge_cols = ["corp_code", "bizr_no", "ceo_nm", "adres", "phn_no", "fax_no"]
    overview = overview.loc[:, [c for c in merge_cols if c in overview.columns]].drop_duplicates(subset=["corp_code"])

    out = out.merge(overview, on="corp_code", how="left", suffixes=("", "_overview"))

    for col in ["bizr_no", "ceo_nm", "adres", "phn_no", "fax_no"]:
        overview_col = f"{col}_overview"
        if overview_col in out.columns:
            if col in out.columns:
                out[col] = out[col].fillna("")
                out[overview_col] = out[overview_col].fillna("")
                out[col] = out[col].where(out[col].astype(str).str.strip() != "", out[overview_col])
            else:
                out[col] = out[overview_col]
            out = out.drop(columns=[overview_col])

        if col not in out.columns:
            out[col] = ""

    return out


def merge_estk_detail_columns(df_base: pd.DataFrame, dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = df_base.copy()

    key_candidates = ["rcept_no", "corp_code", "corp_name", "stock_code", "stock_name"]
    wanted_cols = ["stksen", "stkcnt", "fv", "slprc", "slta"]

    for _, df_part in dfs.items():
        if df_part is None or df_part.empty:
            continue

        available_value_cols = [c for c in wanted_cols if c in df_part.columns]
        if not available_value_cols:
            continue

        common_keys = [c for c in key_candidates if c in out.columns and c in df_part.columns]
        if not common_keys:
            continue

        merge_cols = common_keys + available_value_cols
        part = df_part.loc[:, merge_cols].copy().drop_duplicates()

        out = out.merge(part, on=common_keys, how="left", suffixes=("", "_part"))

        for col in available_value_cols:
            part_col = f"{col}_part"
            if part_col in out.columns:
                if col in out.columns:
                    out[col] = out[col].fillna("").astype(str)
                    out[part_col] = out[part_col].fillna("").astype(str)
                    out[col] = out[col].where(out[col].str.strip() != "", out[part_col])
                else:
                    out[col] = out[part_col]
                out = out.drop(columns=[part_col])

    return out


def _build_report_df(api_key: str, bgn_de: str, end_de: str, report_name: str, report_filter_text: str | None,
                     page_count: int, timeout: int, verify_ssl: bool) -> pd.DataFrame:
    filtered = [
        item
        for item in iter_list(api_key, bgn_de, end_de, page_count=page_count, timeout=timeout, verify_ssl=verify_ssl)
        if report_name in (item.get("report_nm") or "")
    ]
    report_df = pd.DataFrame(filtered)
    if report_filter_text and not report_df.empty:
        report_df = report_df.loc[report_df.report_nm.str.contains(report_filter_text, na=False)]
    if not report_df.empty:
        report_df["URL"] = report_df["rcept_no"].apply(
            lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
        )
    return report_df


def _build_check_list(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame()

    check_list = report_df.loc[
        report_df.report_nm.str.contains(r"\[기재정정\]|\[발행조건확정\]", na=False),
        ["corp_name", "report_nm", "rcept_dt", "rcept_no", "URL"],
    ].rename(columns=MAP_DICT)

    if check_list.empty:
        return check_list

    check_list = check_list.sort_values(
        by=["회사명", "rcept_dt", "접수번호"],
        ascending=[True, False, False],
        kind="mergesort",
    )

    def pick_latest(group: pd.DataFrame) -> pd.DataFrame:
        has_final = group["보고서명"].str.contains(r"\[발행조건확정\]", na=False)
        if has_final.any():
            return group.loc[has_final].head(1)
        return group.head(1)

    check_list = (
        check_list.groupby("회사명", as_index=False, sort=False)
        .apply(pick_latest)
        .reset_index(drop=True)
    )

    check_list = check_list.drop(columns=[c for c in ["rcept_dt", "접수번호"] if c in check_list.columns])
    return check_list


def _write_excel(
    file_obj,
    check_list: pd.DataFrame,
    df_base: pd.DataFrame,
    df_2: pd.DataFrame,
    df_3: pd.DataFrame,
    df_4: pd.DataFrame,
    df_5: pd.DataFrame,
):
    with pd.ExcelWriter(file_obj, engine="xlsxwriter") as writer:
        check_list.to_excel(writer, index=False, sheet_name="검토목록")
        df_base.to_excel(writer, index=False, sheet_name="일반사항")
        # df_5.to_excel(writer, index=False, sheet_name="인수인정보")
        # df_2.to_excel(writer, index=False, sheet_name="자금의사용목적")
        # df_3.to_excel(writer, index=False, sheet_name="매출인에관한사항")
        # df_4.to_excel(writer, index=False, sheet_name="일반청약차환매청구권")

        workbook = writer.book
        highlight_fmt = workbook.add_format({"bg_color": "#F8D7DA"})

        if not check_list.empty and "회사명" in check_list.columns:
            chk_name_col = check_list.columns.get_loc("회사명")
            chk_col_letter = xl_col_to_name(chk_name_col)
            chk_last_row = len(check_list) + 1
            chk_range = f"'검토목록'!${chk_col_letter}$2:${chk_col_letter}${chk_last_row}"
        else:
            chk_range = None

        def apply_highlight(df: pd.DataFrame, sheet_name: str, name_col: str = "회사명"):
            if df.empty or name_col not in df.columns or not chk_range:
                return

            ws = writer.sheets[sheet_name]
            name_col_idx = df.columns.get_loc(name_col)
            col_letter = xl_col_to_name(name_col_idx)

            first_row = 1
            last_row = len(df)
            last_col = len(df.columns) - 1

            if last_row < first_row:
                return

            formula = f"=ISNUMBER(MATCH(${col_letter}{first_row+1},{chk_range},0))"
            ws.conditional_format(
                first_row,
                0,
                last_row,
                last_col,
                {
                    "type": "formula",
                    "criteria": formula,
                    "format": highlight_fmt,
                },
            )

        apply_highlight(check_list, "검토목록")
        apply_highlight(df_base, "일반사항")
        # apply_highlight(df_5, "인수인정보")
        # apply_highlight(df_2, "자금의사용목적")
        # apply_highlight(df_3, "매출인에관한사항")
        # apply_highlight(df_4, "일반청약차환매청구권")

        def set_url_width(df: pd.DataFrame, sheet_name: str, col_name: str = "URL", width: int = 53):
            if col_name not in df.columns:
                return
            ws = writer.sheets[sheet_name]
            col_idx = df.columns.get_loc(col_name)
            ws.set_column(col_idx, col_idx, width)

        set_url_width(check_list, "검토목록")
        set_url_width(df_base, "일반사항")

        # 핵심: 숨길 시트가 active 시트이면 안 됨
        if "일반사항" in writer.sheets:
            writer.sheets["일반사항"].activate()

        if "검토목록" in writer.sheets:
            writer.sheets["검토목록"].hide()

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
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

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

    base_list, df2_list, df3_list, df4_list, df5_list = [], [], [], [], []

    corp_codes = report_df.corp_code.dropna().unique().tolist()
    for corp_code in corp_codes:
        time.sleep(sleep_sec)
        url = "https://opendart.fss.or.kr/api/estkRs.json"
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": (datetime.strptime(bgn_de, "%Y%m%d") - relativedelta(months=6)).strftime("%Y%m%d"),
            "end_de": end_de,
        }

        resp = requests.get(url, params=params, timeout=request_timeout)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        message = data.get("message")
        if status != "000":
            print(f"[SKIP] {corp_code} API error {status}: {message}")
            continue

        groups = data.get("group", [])
        dfs: dict[str, pd.DataFrame] = {}
        for g in groups:
            title = g.get("title", "group")
            items = g.get("list", [])
            dfs[title] = pd.DataFrame(items)

        df_base = dfs.get("일반사항")
        if df_base is None or df_base.empty:
            print(f"[SKIP] {corp_code} 일반사항 없음")
            continue

        try:
            overview = get_company_overview_fields(api_key, corp_code)
        except Exception:
            print(f"[WARN] {corp_code} 기업개요 없음")
            overview = get_empty_company_overview_fields(corp_code)

        df_kind = dfs.get("증권의종류")
        if df_kind is None or df_kind.empty:
            df_base = df_base.copy()
        else:
            df_base = pd.merge(df_base, df_kind)

        df_base = merge_estk_detail_columns(df_base, dfs)
        df_base = merge_company_overview(df_base, overview)
        df_base["URL"] = df_base["rcept_no"].apply(
            lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
        )

        df_2 = dfs.get("자금의사용목적", pd.DataFrame())
        df_3 = dfs.get("매출인에관한사항", pd.DataFrame())
        df_4 = dfs.get("일반청약자환매청구권", pd.DataFrame())
        df_5 = dfs.get("인수인정보", pd.DataFrame())

        df_base = df_base.rename(columns=MAP_DICT)
        if "사업자등록번호" in df_base.columns:
            df_base["사업자등록번호"] = df_base["사업자등록번호"].apply(format_bizr_no)
        df_2 = df_2.rename(columns=MAP_DICT)
        df_3 = df_3.rename(columns=MAP_DICT)
        df_4 = df_4.rename(columns=MAP_DICT)
        df_5 = df_5.rename(columns=MAP_DICT)

        base_list.append(df_base)
        df2_list.append(df_2)
        df3_list.append(df_3)
        df4_list.append(df_4)
        df5_list.append(df_5)

    def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates()

    df_base = _safe_concat(base_list)
    df_2 = _safe_concat(df2_list)
    df_3 = _safe_concat(df3_list)
    df_4 = _safe_concat(df4_list)
    df_5 = _safe_concat(df5_list)

    if not df_base.empty and "사업자등록번호" not in df_base.columns:
        df_base["사업자등록번호"] = ""

    sort_cols = [
        "회사명",
        "사업자등록번호",
        "상장구분",
#        "증권의종류",
        "증권수량",
#        "액면가액",
        "모집(매출)가액",
        "모집(매출)총액",
        "청약기일",
        "납입기일",
#        "청약공고일",
#        "배정공고일",
#        "배정기준일",
        "대표자명",
        "주소",
        "전화번호",
#        "팩스번호",
        '발행회사 담당',
        '실무담당',
        '당사 연락처(비고)',
        "URL",
    ]

    if not df_base.empty:
        df_base['발행회사 담당'] = ''
        df_base['실무담당'] = ''
        df_base['당사 연락처(비고)'] = ''

        df_base = df_base.loc[:, [c for c in sort_cols if c in df_base.columns]]
        if "상장구분" in df_base.columns:
            df_base["상장구분"] = df_base["상장구분"].map(CORP_CLS_MAP)

    def _drop_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        drop = [c for c in cols if c in df.columns]
        if drop:
            df = df.drop(columns=drop)
        return df

    df_2 = _drop_cols(df_2, ["접수번호", "고유번호", "상장구분"])
    df_3 = _drop_cols(df_3, ["접수번호", "고유번호", "상장구분"])
    df_4 = _drop_cols(df_4, ["접수번호", "고유번호", "상장구분"])
    df_5 = _drop_cols(df_5, ["접수번호", "고유번호", "상장구분"]) 

    if not df_base.empty and "납입기일" in df_base.columns:
        df_base = df_base.sort_values(by="납입기일", ascending=False, kind="mergesort")
    elif not df_base.empty:
        df_base = df_base.sort_values(by="회사명", ascending=True, kind="mergesort")

    company_order = []
    if not df_base.empty and "회사명" in df_base.columns:
        company_order = df_base["회사명"].dropna().drop_duplicates().tolist()

    def _sort_by_company_order(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "회사명" not in df.columns or not company_order:
            return df
        cat = pd.Categorical(df["회사명"], categories=company_order, ordered=True)
        df = df.assign(_회사명_order=cat)
        return df.sort_values(by="_회사명_order", kind="mergesort").drop(columns="_회사명_order").reset_index(drop=True)

    df_base = df_base.reset_index(drop=True) if not df_base.empty else df_base
    df_2 = _sort_by_company_order(df_2)
    df_3 = _sort_by_company_order(df_3)
    df_4 = _sort_by_company_order(df_4)
    df_5 = _sort_by_company_order(df_5)
    check_list = _sort_by_company_order(check_list) if not check_list.empty else check_list

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    kst_now = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M")
    out_path = out_dir / f"DART_증권신고서_지분증권_F{bgn_de}_T{end_de}_추출시간_{kst_now}.xlsx"

    _write_excel(out_path, check_list, df_base, df_2, df_3, df_4, df_5)
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

    base_list, df2_list, df3_list, df4_list, df5_list = [], [], [], [], []

    corp_codes = report_df.corp_code.dropna().unique().tolist()
    for corp_code in corp_codes:
        time.sleep(sleep_sec)
        url = "https://opendart.fss.or.kr/api/estkRs.json"
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": (datetime.strptime(bgn_de, "%Y%m%d") - relativedelta(months=6)).strftime("%Y%m%d"),
            "end_de": end_de,
        }

        resp = requests.get(url, params=params, timeout=request_timeout)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        message = data.get("message")
        if status != "000":
            print(f"[SKIP] {corp_code} API error {status}: {message}")
            continue

        groups = data.get("group", [])
        dfs: dict[str, pd.DataFrame] = {}
        for g in groups:
            title = g.get("title", "group")
            items = g.get("list", [])
            dfs[title] = pd.DataFrame(items)

        df_base = dfs.get("일반사항")
        if df_base is None or df_base.empty:
            continue

        try:
            overview = get_company_overview_fields(api_key, corp_code)
        except Exception:
            overview = get_empty_company_overview_fields(corp_code)

        df_kind = dfs.get("증권의종류")
        if df_kind is None or df_kind.empty:
            df_base = df_base.copy()
        else:
            df_base = pd.merge(df_base, df_kind)

        df_base = merge_estk_detail_columns(df_base, dfs)
        df_base = merge_company_overview(df_base, overview)
        df_base["URL"] = df_base["rcept_no"].apply(
            lambda x: f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x}"
        )

        df_2 = dfs.get("자금의사용목적", pd.DataFrame())
        df_3 = dfs.get("매출인에관한사항", pd.DataFrame())
        df_4 = dfs.get("일반청약자환매청구권", pd.DataFrame())
        df_5 = dfs.get("인수인정보", pd.DataFrame())

        df_base = df_base.rename(columns=MAP_DICT)
        if "사업자등록번호" in df_base.columns:
            df_base["사업자등록번호"] = df_base["사업자등록번호"].apply(format_bizr_no)
        df_2 = df_2.rename(columns=MAP_DICT)
        df_3 = df_3.rename(columns=MAP_DICT)
        df_4 = df_4.rename(columns=MAP_DICT)
        df_5 = df_5.rename(columns=MAP_DICT)

        base_list.append(df_base)
        df2_list.append(df_2)
        df3_list.append(df_3)
        df4_list.append(df_4)
        df5_list.append(df_5)

    def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates()

    df_base = _safe_concat(base_list)
    df_2 = _safe_concat(df2_list)
    df_3 = _safe_concat(df3_list)
    df_4 = _safe_concat(df4_list)
    df_5 = _safe_concat(df5_list)

    if not df_base.empty and "사업자등록번호" not in df_base.columns:
        df_base["사업자등록번호"] = ""

    sort_cols = [
        "회사명",
        "사업자등록번호",
        "상장구분",
#        "증권의종류",
        "증권수량",
#        "액면가액",
        "모집(매출)가액",
        "모집(매출)총액",
        "청약기일",
        "납입기일",
#        "청약공고일",
#        "배정공고일",
#        "배정기준일",
        "대표자명",
        "주소",
        "전화번호",
#        "팩스번호",
        '발행회사 담당',
        '실무담당',
        '당사 연락처(비고)',
        "URL",
    ]

    if not df_base.empty:
        df_base['발행회사 담당'] = ''
        df_base['실무담당'] = ''
        df_base['당사 연락처(비고)'] = ''
        df_base = df_base.loc[:, [c for c in sort_cols if c in df_base.columns]]
        if "상장구분" in df_base.columns:
            df_base["상장구분"] = df_base["상장구분"].map(CORP_CLS_MAP)

    def _drop_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        drop = [c for c in cols if c in df.columns]
        if drop:
            df = df.drop(columns=drop)
        return df

    df_2 = _drop_cols(df_2, ["접수번호", "고유번호", "상장구분"])
    df_3 = _drop_cols(df_3, ["접수번호", "고유번호", "상장구분"])
    df_4 = _drop_cols(df_4, ["접수번호", "고유번호", "상장구분"])
    df_5 = _drop_cols(df_5, ["접수번호", "고유번호", "상장구분"])

    if not df_base.empty and "납입기일" in df_base.columns:
        df_base = df_base.sort_values(by="납입기일", ascending=False, kind="mergesort")
    elif not df_base.empty:
        df_base = df_base.sort_values(by="회사명", ascending=True, kind="mergesort")

    company_order = []
    if not df_base.empty and "회사명" in df_base.columns:
        company_order = df_base["회사명"].dropna().drop_duplicates().tolist()

    def _sort_by_company_order(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "회사명" not in df.columns or not company_order:
            return df
        cat = pd.Categorical(df["회사명"], categories=company_order, ordered=True)
        df = df.assign(_회사명_order=cat)
        return df.sort_values(by="_회사명_order", kind="mergesort").drop(columns="_회사명_order").reset_index(drop=True)

    df_base = df_base.reset_index(drop=True) if not df_base.empty else df_base
    df_2 = _sort_by_company_order(df_2)
    df_3 = _sort_by_company_order(df_3)
    df_4 = _sort_by_company_order(df_4)
    df_5 = _sort_by_company_order(df_5)
    check_list = _sort_by_company_order(check_list) if not check_list.empty else check_list

    kst_now = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M")
    filename = f"DART_증권신고서_지분증권_F{bgn_de}_T{end_de}_추출시간_{kst_now}.xlsx"

    buffer = BytesIO()
    _write_excel(buffer, check_list, df_base, df_2, df_3, df_4, df_5)
    return buffer.getvalue(), filename
