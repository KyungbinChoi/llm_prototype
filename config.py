answer_examples = [
    {
        "input": "2020년 11월부터 12월까지 기간 동안의 접속 기록을 바탕으로 디바이스 종류별 접속자 수, 구매자 수, 총 구매액을 집계하는 쿼리를 알려주세요.", 
        "answer": """
    2020년 11월부터 12월까지 기간 동안의 접속 기록을 바탕으로 디바이스 종류별 접속자 수, 구매자 수, 총 구매액을 집계하는 쿼리는 아래와 같습니다.
    빅쿼리 데이터 셋 bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_* 을 예시로 작성된 쿼리입니다.

    -- SQL Query:
    SELECT 
        device.category AS device_type,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchasers,
        SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue_in_usd ELSE 0 END) AS total_revenue_usd
    FROM 
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE 
        _TABLE_SUFFIX BETWEEN '20201101' AND '20201231'
    GROUP BY 
        device.category
    ORDER BY 
        device_type;

    1. 주요 컬럼
    device.category AS device_type:
        device.category 필드는 접속한 디바이스 종류를 나타냅니다. 예: mobile, desktop, tablet.
        이 필드를 기준으로 그룹화하여 디바이스 종류별로 데이터를 집계합니다.
    
    COUNT(DISTINCT user_pseudo_id):
        user_pseudo_id는 사용자를 고유하게 식별하는 ID입니다.
        이를 COUNT(DISTINCT user_pseudo_id)로 계산하여 디바이스별 총 사용자 수를 집계합니다.
    
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END):
        구매 이벤트(event_name = 'purchase')를 발생시킨 고유 사용자 수(구매자 수)를 계산합니다.
        CASE 문을 사용해 구매 이벤트인 경우만 조건을 만족하도록 필터링합니다.
    
    SUM(CAST(event_params.value.int_value AS NUMERIC)):
        구매 이벤트에서 event_params.value에 저장된 구매 금액을 합산하여 디바이스별 총 구매액을 계산합니다.
        금액은 int_value로 저장되므로 NUMERIC으로 변환하여 계산합니다.
    
    2. WHERE 조건
    _TABLE_SUFFIX BETWEEN '20201101' AND '20201231':
        분석 기간을 2020년 11월 1일부터 12월 31일까지로 제한합니다.
        Google Analytics 4 데이터셋은 날짜별로 테이블이 나뉘어 있으므로 _TABLE_SUFFIX를 사용해 테이블 범위를 지정합니다.
    
    event_name IN ('purchase', 'session_start'):
        구매(purchase) 및 세션 시작(session_start) 이벤트만 분석 대상으로 포함합니다.
    
    event_params.key = 'value':
        구매 이벤트의 구매 금액은 event_params 배열 내에 key = 'value'로 저장됩니다.
        이를 필터링하여 구매 금액 데이터를 가져옵니다.
    
    3. COALESCE
    COALESCE(total_revenue, 0):
        총 구매액(total_revenue)이 NULL인 경우 0으로 대체합니다.
    4. ORDER BY
    ORDER BY total_revenue DESC:
        총 구매액을 기준으로 내림차순 정렬합니다.
    """
    },
    {
        "input": "2020년 11월부터 12월까지 기간 동안의 접속 기록을 바탕으로 일자별 DAU(daily active user), 총 세션 수 , 구매 전환율을 집계하는 쿼리를 알려주세요.", 
        "answer": """
        2020년 11월부터 12월까지 기간 동안의 접속 기록을 바탕으로 일자별 DAU(daily active user), 총 세션 수 , 구매 전환율을 집계하는 쿼리는 아래와 같습니다.
    빅쿼리 데이터 셋 bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_* 을 예시로 작성된 쿼리입니다.
    
    - SQL Query : 
    WITH daily_metrics AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_pseudo_id) AS dau,
        COUNTIF(event_name = 'session_start') AS sessions,
        COUNTIF(event_name = 'purchase') AS purchases
    FROM
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
        _TABLE_SUFFIX BETWEEN '20201101' AND '20201231'  -- 분석할 기간 설정
    GROUP BY
        event_date
    ),
    conversion_rate AS (
    SELECT
        event_date,
        dau,
        sessions,
        purchases,
        SAFE_DIVIDE(purchases, sessions) AS purchase_conversion_rate
    FROM
        daily_metrics
    )
    SELECT
        event_date,
        dau,
        sessions,
        purchases,
        ROUND(purchase_conversion_rate * 100, 2) AS purchase_conversion_rate_percentage  -- 백분율로 변환
    FROM
        conversion_rate
    ORDER BY
        event_date;

    - 쿼리 설명
    1. WITH daily_metrics:
        event_date: 이벤트 발생 날짜.
        COUNT(DISTINCT user_pseudo_id): DAU(Daily Active Users) 계산.
        COUNTIF(event_name = 'session_start'): 세션(Session) 시작 이벤트 수 계산.
        COUNTIF(event_name = 'purchase'): 구매(Purchase) 이벤트 수 계산.
    2. WITH conversion_rate:
        SAFE_DIVIDE(purchases, sessions): 세션 수 대비 구매 수를 계산하여 구매 전환율(Purchase Conversion Rate)을 산출. SAFE_DIVIDE를 사용해 0으로 나누는 경우를 방지.
    3. SELECT:
        ROUND(purchase_conversion_rate * 100, 2): 구매 전환율을 백분율로 변환하고 소수점 둘째 자리까지 반올림.
        결과를 event_date 기준으로 정렬.

        
    """
    },
    {
        "input": "2020년 11월부터 2021년 1월까지 접속 국가별로 사용자 수, 구매자 수, 총 구매액을 집계하는 쿼리를 어떻게 작성할 수 있나요?", 
        "answer": """
        2020년 11월부터 2021년 1월까지 접속 국가별로 사용자 수, 구매자 수, 총 구매액을 집계하는 쿼리는 아래와 같습니다.
        빅쿼리 데이터 셋 bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_* 을 예시로 작성된 쿼리입니다.
        - SQL Query:
        WITH user_metrics AS (
        SELECT
            geo.country AS country,
            COUNT(DISTINCT user_pseudo_id) AS total_users,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchasers,
            SUM(CAST(event_params.value.int_value AS NUMERIC)) AS total_revenue
        FROM
            `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
            UNNEST(event_params) AS event_params
        WHERE
            _TABLE_SUFFIX BETWEEN '20201101' AND '20210131' -- 2020년 11월부터 2021년 1월까지
            AND event_name IN ('purchase')  -- 구매 이벤트만 필터링
            AND event_params.key = 'value' -- 구매 금액 관련 파라미터 필터링
        GROUP BY
            geo.country
        )
        SELECT
        country,
        total_users,
        purchasers,
        COALESCE(total_revenue, 0) AS total_revenue
        FROM
        user_metrics
        ORDER BY
        total_revenue DESC;
        
        쿼리 설명
        1. WITH user_metrics:

            geo.country AS country:
                geo.country 필드를 사용하여 국가별로 그룹화합니다.
            COUNT(DISTINCT user_pseudo_id) AS total_users:
                각 국가별 고유 사용자 수(총 사용자 수)를 계산합니다.
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id END) AS purchasers:
                구매 이벤트(event_name = 'purchase')를 발생시킨 고유 사용자 수(구매자 수)를 계산합니다.
            SUM(CAST(event_params.value.int_value AS NUMERIC)) AS total_revenue:
                구매 이벤트에서 event_params.value 파라미터를 합산하여 총 구매 금액을 계산합니다.
        
        2. UNNEST(event_params):
            이벤트 파라미터(event_params) 필드는 배열 형태로 저장되어 있으므로 UNNEST를 사용하여 각 키-값 쌍을 펼칩니다.
        
        3. WHERE 조건:
            _TABLE_SUFFIX BETWEEN '20201101' AND '20210131':
                2020년 11월 1일부터 2021년 1월 31일까지의 데이터를 필터링합니다.
            event_name IN ('purchase'):
                구매 이벤트만 포함합니다.
            event_params.key = 'value':
                구매 금액을 나타내는 파라미터만 필터링합니다.
            COALESCE(total_revenue, 0):
                구매 금액이 NULL인 경우 0으로 대체합니다.
        
        4. ORDER BY total_revenue DESC:
            총 구매 금액이 높은 순서대로 결과를 정렬합니다.
        
        """
    },
]