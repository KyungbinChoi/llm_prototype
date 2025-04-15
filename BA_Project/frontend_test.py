# frontend.py
import time, re, os
import pandas as pd
from typing import Tuple, List
import streamlit as st
from backend_test import get_raw_df, get_pivot_df, get_fact_numbers, get_days_raw_df, prepare_weekday_comparison, get_weekday_diff_summary
from dotenv import load_dotenv
from langchain_huggingface import ChatHuggingFace, HuggingFacePipeline
from langchain_core.messages import HumanMessage
from transformers import BitsAndBytesConfig

# ----------------------------
# 모델 초기화 (최초 1회 실행)
# ----------------------------
@st.cache_resource
def load_llm():
    load_dotenv()
    hf_token = os.getenv("HF_TOKEN")
    
    quantization_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype="float16",
    bnb_4bit_use_double_quant=True,
    )
    
    chat_model = HuggingFacePipeline.from_model_id(
        model_id='LGAI-EXAONE/EXAONE-3.0-7.8B-Instruct',
        task='text-generation',
        device_map="auto",
        pipeline_kwargs=dict(
            max_new_tokens=3000,
            do_sample=False,
            repetition_penalty=1.03
        ),
        model_kwargs={'quantization_config': quantization_config,'trust_remote_code': True}
    )
    llm = ChatHuggingFace(llm=chat_model)
    return llm

llm = load_llm()


# @st.cache_data
raw_df = get_raw_df()
raw_day_df = get_days_raw_df()
pivot_df = get_pivot_df(raw_df)

def extract_week(text):
    match = re.search(r"(20\d{2}-W\d{2})(?!\d)", text)
    return match.group(1) if match else None


# Streamlit UI
st.title("📊 Weekly Market Share 요약 챗봇")
st.markdown("주간 기준 KPI를 요약하고 싶은 주차를 포함한 질문을 입력해주세요.")

# 세션 상태 초기화
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []  # [(질문, 응답)]
if "last_kpi_week" not in st.session_state:
    st.session_state.last_kpi_week = None

# 이전 히스토리의 출력
if st.session_state.chat_history:
    st.markdown("### 💬 이전 대화")
    for idx, (q, a) in enumerate(st.session_state.chat_history):
        with st.chat_message("user"):
            st.markdown(q)
        with st.chat_message("assistant"):
            st.markdown(a)

user_query = st.text_input("질문 입력", placeholder="예: 2025-W13 기준 KPI 요약해줘")

if user_query:
    week = extract_week(user_query)
    if not week:
        if st.session_state.last_kpi_week:
            week = st.session_state.last_kpi_week
        else:
            st.error("입력한 질문에서 기준 주차를 찾을 수 없습니다. 예: '2025-W13'")
            st.stop()

    try:
        (latest, fact_list) = get_fact_numbers(pivot_df, week)
        fact_b, fact_c, fact_y = fact_list[0], fact_list[1], fact_list[2]
        total_cnt = latest["CNT_TOTAL"]

        # 요일별 KPI 비교 요약
        weekday_comp = prepare_weekday_comparison(raw_day_df, week)
        weekday_summary = get_weekday_diff_summary(weekday_comp, week)

        prev_answer = st.session_state.chat_history[-1][1] if st.session_state.chat_history else ""

        prompt = f"""
        [업무 목적] 당신은 Y사의 유능한 데이터 분석가입니다.
        다음은 각 배달 플랫폼 3사의 주간 KPI 수치입니다.
        3개 기업 별로 각각 전주 대비 매출, 주문수, 점유율의 변동을 계산하고 요약해주세요. 
        또한 3개 기업 별로 월요일부터 일요일까지의 점유율 흐름을 분석하고 전주와의 차이를 계산해 요약할 필요가 있습니다.
        
        [지시사항]
        아래 주간 요약 및 요일별 표는 분석을 위한 참고용 데이터입니다.
        표를 그대로 나열하거나 복사하지 말고 답변은 반드시 당신이 직접 데이터를 분석한 것처럼 요약/정리해주세요.
        요일별 분석시 모든 요일에 대해 전주 대비 증감의 정도와 추세 등의 정보를 요약해야합니다.
        
        [응답 형식 지침]
        - 전체 응답은 5~7개의 단락으로 요약하고 요점을 정리해주세요.
        - 점유율의 변동은 퍼센트(%)와 퍼센트 포인트(%p) 모두 계산해야 합니다.
        - 주요 포인트를 요약할 때 반드시 구체적인 수치를 함께 기입해야 합니다.
        - 계산 과정은 생략해주세요.
        - 특히 점유율의 변화나 경쟁사와의 격차 확대 등에 주목해 주세요.
        - 마지막 요약 시 기업에 대한 종합적인 평가는 생략해주세요.
        
        [이전 요약]
        {prev_answer}

        [주간 KPI 수치 요약 - {latest['base_week']} 기준]
        {fact_b}
        {fact_c}
        {fact_y}
        전체 주문수: {total_cnt:,}건

        [요일별 KPI 비교 요약]
        다음은 각 기업별로 이번 주 월요일부터 일요일까지의 GMV 및 주문수 점유율(%)과, 지난 주 대비 점유율 변화(%p)를 요약한 표입니다.
        
        {weekday_summary}
        
        [질문]
        {user_query}
        """.strip()

        with st.spinner("요약 중입니다..."):
            response = llm.invoke([HumanMessage(content=prompt)])
            raw_text = response.content.strip()

            if "[|assistant|]" in raw_text:
                answer = raw_text.split("[|assistant|]", 1)[-1].strip()
            else:
                answer = raw_text

            # 현재 질문/응답도 화면에 추가 출력
            with st.chat_message("user"):
                st.markdown(user_query)
            with st.chat_message("assistant"):
                placeholder = st.empty()
                typed_text = ""
                for char in answer:
                    typed_text += char
                    placeholder.markdown(typed_text)
                    time.sleep(0.01)

            # 세션 상태에 기록
            st.session_state.chat_history.append((user_query, answer))
            st.session_state.last_kpi_week = week

    except Exception as e:
        st.error(f"오류 발생: {str(e)}")