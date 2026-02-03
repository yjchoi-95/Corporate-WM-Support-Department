from datetime import date, timedelta

import streamlit as st

from ri_pipeline import run_rights_issue_report_bytes


st.set_page_config(page_title="법인WM지원부_DART 전자공시 수집 자동화", layout="centered")
st.title("🏢 DART 전자공시 수집 자동화")

tabs = st.tabs(["유상증자"])
with tabs[0]:
    api_key = st.secrets["DART_API_KEY"]

    today = date.today()
    if "bgn_date" not in st.session_state:
        st.session_state["bgn_date"] = today - timedelta(days=7)
    if "end_date" not in st.session_state:
        st.session_state["end_date"] = today

    bgn_min = st.session_state["end_date"] - timedelta(days=45)
    bgn_max = min(st.session_state["end_date"] + timedelta(days=45), today)
    bgn_date = st.date_input(
        "시작일자",
        min_value=bgn_min,
        max_value=bgn_max,
        key="bgn_date",
    )

    end_min = bgn_date - timedelta(days=45)
    end_max = min(bgn_date + timedelta(days=45), today)
    end_date = st.date_input(
        "종료일자",
        min_value=end_min,
        max_value=end_max,
        key="end_date",
    )

    if st.button("실행", type="primary"):
        if not api_key:
            st.error("API KEY를 입력하세요.")
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
                )
            else:
                st.warning("조회 결과가 없습니다.")
