from datetime import date, timedelta

import streamlit as st

from major_report_pipeline import run_major_paid_increase_report_bytes
from ri_pipeline import run_rights_issue_report_bytes


st.set_page_config(page_title="법인WM지원부_DART 전자공시 수집 자동화", layout="centered")
st.title("🏢 DART 전자공시 수집 자동화")

def _render_date_inputs(key_prefix: str):
    today = date.today()
    bgn_key = f"{key_prefix}_bgn_date"
    end_key = f"{key_prefix}_end_date"

    if bgn_key not in st.session_state:
        st.session_state[bgn_key] = today - timedelta(days=7)
    if end_key not in st.session_state:
        st.session_state[end_key] = today

    bgn_min = st.session_state[end_key] - timedelta(days=45)
    bgn_max = min(st.session_state[end_key] + timedelta(days=45), today)
    bgn_date = st.date_input(
        "시작일자",
        min_value=bgn_min,
        max_value=bgn_max,
        key=bgn_key,
    )

    end_min = bgn_date - timedelta(days=45)
    end_max = min(bgn_date + timedelta(days=45), today)
    end_date = st.date_input(
        "종료일자",
        min_value=end_min,
        max_value=end_max,
        key=end_key,
    )

    return bgn_date, end_date

def _get_api_key() -> str:
    return st.secrets["DART_API_KEY"]

tabs = st.tabs(["유상증자", "주요사항보고서"])

with tabs[0]:
    api_key = _get_api_key()
    bgn_date, end_date = _render_date_inputs("rights")

    if st.button("실행", type="primary", key="run_rights"):
        if not api_key:
            st.error("DART_API_KEY가 설정되지 않았습니다.")
        elif bgn_date > end_date:
            st.error("시작일자는 종료일자보다 이후일 수 없습니다.")
        else:
            with st.spinner("조회 중..."):
                result = run_rights_issue_report_bytes(
                    api_key=api_key,
                    bgn_de=bgn_date.strftime("%Y%m%d"),
                    end_de=end_date.strftime("%Y%m%d"),
                )

            if result:
                data, filename = result
                st.success("완료")
                st.download_button(
                    "엑셀 다운로드",
                    data=data,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_rights",
                )
            else:
                st.warning("조회 결과가 없습니다.")

with tabs[1]:
    api_key = _get_api_key()
    bgn_date, end_date = _render_date_inputs("major")

    if st.button("실행", type="primary", key="run_major"):
        if not api_key:
            st.error("DART_API_KEY가 설정되지 않았습니다.")
        elif bgn_date > end_date:
            st.error("시작일자는 종료일자보다 이후일 수 없습니다.")
        else:
            with st.spinner("조회 중..."):
                result = run_major_paid_increase_report_bytes(
                    api_key=api_key,
                    bgn_de=bgn_date.strftime("%Y%m%d"),
                    end_de=end_date.strftime("%Y%m%d"),
                )

            if result:
                data, filename = result
                st.success("완료")
                st.download_button(
                    "엑셀 다운로드",
                    data=data,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_major",
                )
            else:
                st.warning("조회 결과가 없습니다.")

