from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_openai import ChatOpenAI
from langchain.chains import create_history_aware_retriever, create_retrieval_chain, RetrievalQA
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_core.prompts import FewShotChatMessagePromptTemplate
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from config import answer_examples

store= {}

def get_session_history(session_id: str) -> BaseChatMessageHistory:
    if session_id not in store:
        store[session_id] = ChatMessageHistory()
    return store[session_id]


def get_retriever():
    embedding = OpenAIEmbeddings(model = 'text-embedding-3-large')
    index_name = 'ga4-sql-index'
    database = PineconeVectorStore.from_existing_index(index_name=index_name, embedding= embedding)
    retriever = database.as_retriever(search_kwargs= {'k':5})
    return retriever

def get_llm(model ='gpt-4o'):
    llm = ChatOpenAI(model=model)
    return llm


# def get_llm_chain():
#     llm = get_llm()
#     prompt = ChatPromptTemplate.from_template(f"""
#     [identity]
#     당신은 데이터 과학자이자 데이터 분석가입니다. 주어진 데이터에 대한 정보를 바탕으로 데이터를 활용하고자 하는 사람들의 질문에 답할 수 있는 쿼리를 작성할 수 있습니다. 
#     특히 Bigquery 환경에서 필요한 테이블을 추출하는데 어려움이 없습니다.
#     사용자의 질문을 바탕으로 답변을 작성해주세요.
#     질문: {{question}}
#     """)
#     llm_chain = prompt | llm | StrOutputParser()
#     return llm_chain


def get_history_retriever():
    llm = get_llm()
    retriever = get_retriever()
    
    contextualize_q_system_prompt = (
        "Given a chat history and the latest user question "
        "which might reference context in the chat history, "
        "formulate a standalone question which can be understood "
        "without the chat history. Do NOT answer the question, "
        "just reformulate it if needed and otherwise return it as is."
    )

    contextualize_q_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", contextualize_q_system_prompt),
            MessagesPlaceholder("chat_history"),
            ("human", "{input}"),
        ]
    )
    
    history_aware_retriever = create_history_aware_retriever(
        llm, retriever, contextualize_q_prompt
    )
    return history_aware_retriever


def get_rag_chain():
    llm = get_llm()

    example_prompt = ChatPromptTemplate.from_messages(
        [
            ("human", "{input}"),
            ("ai", "{answer}")
        ]
    )
    few_shotprompt = FewShotChatMessagePromptTemplate(
        example_prompt =example_prompt,
        examples = answer_examples
    )

    qa_system_prompt = """
    당신은 데이터 과학자이자 데이터 분석가입니다. 주어진 데이터에 대한 정보를 바탕으로 데이터를 활용하고자 하는 사람들의 질문에 답할 수 있는 쿼리를 작성할 수 있습니다. 
    특히 Bigquery 환경에서 필요한 테이블을 추출하는데 어려움이 없습니다.
    사용자의 질문에 맞는 전체 쿼리를 작성해주고 어떻게 쿼리가 작성된 건지 추가로 설명해주시기 바랍니다.
    
    {context}
    """
    qa_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", qa_system_prompt),
            few_shotprompt,
            MessagesPlaceholder("chat_history"),
            ("human", "{input}"),
        ]
    )

    question_answer_chain = create_stuff_documents_chain(llm, qa_prompt)
    history_aware_retriever = get_history_retriever()
    rag_chain = create_retrieval_chain(history_aware_retriever, question_answer_chain)
    conversational_rag_chain = RunnableWithMessageHistory(
        rag_chain,
        get_session_history,
        input_messages_key="input",
        history_messages_key="chat_history",
        output_messages_key="answer",
    ).pick('answer')
    
    return conversational_rag_chain


def get_ai_response(user_message):
    # rag_chain = get_rag_chain()
    query_chain = get_rag_chain()

    ai_response = query_chain.stream(
        {
            "input" : user_message
        }, config = {
            "configurable":{"session_id":"abc123"}
        }
    )

    return ai_response