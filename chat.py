import streamlit as st
from llm import get_ai_response
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title='GA4 LOG Data Query Chatbot', page_icon = "⚡️🤖⚡️")
st.title('⚡️GA4 Bigquery 챗봇⚡️')
st.caption('GA4 로그 데이터에서 궁금한 데이터를 추출하기 위한 Query 를 답변해드립니다!')

if 'message_list' not in st.session_state:
    st.session_state.message_list = []

for message in st.session_state.message_list:
    with st.chat_message(message["role"]):
        st.write(message["content"])

if user_question := st.chat_input(placeholder = "GA4 로그 데이터 분석을 위한 질문을 적어주세요. 필요한 데이터 추출을 위한 Query를 생성합니다."):
    with st.chat_message("user"):
        st.write(user_question)
    st.session_state.message_list.append({"role":"user", "content":user_question})
    with st.spinner("답변을 생성하는 중입니다."):
        ai_response = get_ai_response(user_message=user_question)
        with st.chat_message("ai"):
            ai_message =st.write_stream(ai_response)
            st.session_state.message_list.append({"role":"ai", "content":ai_message})
