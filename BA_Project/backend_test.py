import os
os.chdir('../')
import pandas as pd
import numpy as np
pd.options.display.float_format = '{:.3f}'.format
from IPython.display import display
from DreamTF.utils import query_reader
from typing import Tuple, List
os.chdir('./LLM_project')


def get_raw_df():
    query_text = """
    /* MS_Weekly_all */

    SELECT 
        CASE
          WHEN etl_base_dt < '2023-03-13'                                         --과거 데이터는 일괄 적재라 etl_base_dt 가 똑같음
          THEN CONCAT(CAST(TA_YY AS INT), "-W", FORMAT('%02d',CAST(WEEK AS INT))) --따라서 TA_YY, WEEK 컬럼값 그대로 사용
          ELSE FORMAT_DATE('%G-W%V', etl_base_dt)                                 --최근 데이터는 매주 적재되므로 etl_base_dt 에서 isoweek 추출
          END AS base_week
      , CASE
          WHEN BRD_NM = 'C사' THEN 'CE'
          WHEN BRD_NM = 'Y사' THEN 'YGY'
          WHEN BRD_NM = 'B사' THEN 'BDMJ' 
          ELSE '기타' END AS BRD_NM
      , CASE
          WHEN etl_base_dt < '2023-03-13'
          THEN CAST(TA_YY AS INT)
          ELSE CAST(FORMAT_DATE('%G', etl_base_dt) AS INT)
          END AS YEAR
      , CASE
          WHEN etl_base_dt < '2023-03-13'
          THEN FORMAT('%02d',CAST(WEEK AS INT))
          ELSE FORMAT('%02d',EXTRACT(ISOWEEK FROM etl_base_dt))
          END AS WEEK

      , SUM(EST_HGA) AS AMT
      , SUM(EST_CNT) AS CNT

    FROM `ygy-datawarehouse.auth_view.avw_external_shinhan_monitoring_weekly_region`
    WHERE 1=1
    AND CAST(TA_YY AS INT)>=2022
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
    """
    query_job = query_reader("")
    query_job.read_query_text(query_text, query_raw=True)
    ms_df = query_job.to_pandas()
    result_pivot = ms_df.groupby(['base_week','BRD_NM'])[['AMT','CNT']].max().unstack().reset_index()
    result_pivot.columns = ['base_week','AMT_BDMJ','AMT_CE', 'AMT_YGY', 'CNT_BDMJ', 'CNT_CE', 'CNT_YGY']

    for c in result_pivot.columns[1:]:
        result_pivot[c] = result_pivot[c].astype(int)
        
    return result_pivot


def get_pivot_df(result_pivot: pd.DataFrame) -> pd.DataFrame:
    result_pivot["AMT_TOTAL"] = result_pivot[["AMT_BDMJ", "AMT_CE", "AMT_YGY"]].sum(axis=1)
    result_pivot["CNT_TOTAL"] = result_pivot[["CNT_BDMJ", "CNT_CE", "CNT_YGY"]].sum(axis=1)

    result_pivot["SHARE_CNT_BDMJ"] = (result_pivot["CNT_BDMJ"] / result_pivot["CNT_TOTAL"] * 100).round(2)
    result_pivot["SHARE_CNT_CE"] = (result_pivot["CNT_CE"] / result_pivot["CNT_TOTAL"] * 100).round(2)
    result_pivot["SHARE_CNT_YGY"] = (result_pivot["CNT_YGY"] / result_pivot["CNT_TOTAL"] * 100).round(2)

    result_pivot["SHARE_AMT_BDMJ"] = (result_pivot["AMT_BDMJ"] / result_pivot["AMT_TOTAL"] * 100).round(2)
    result_pivot["SHARE_AMT_CE"] = (result_pivot["AMT_CE"] / result_pivot["AMT_TOTAL"] * 100).round(2)
    result_pivot["SHARE_AMT_YGY"] = (result_pivot["AMT_YGY"] / result_pivot["AMT_TOTAL"] * 100).round(2)

    return result_pivot


def get_fact_numbers(df: pd.DataFrame, target_week: str) -> Tuple[pd.Series, List[str]]:
    df_sorted = df.sort_values("base_week").reset_index(drop=True)

    if target_week not in df_sorted["base_week"].values:
        raise ValueError(f"선택한 주차({target_week})가 데이터에 존재하지 않습니다.")

    target_idx = df_sorted[df_sorted["base_week"] == target_week].index[0]

    if target_idx == 0:
        raise ValueError(f"'{target_week}'은 첫 번째 주차로, 비교 가능한 전주 데이터가 없습니다.")

    latest = df_sorted.iloc[target_idx]
    previous = df_sorted.iloc[target_idx - 1]

    def generate_company_facts(company, col_prefix_amt, col_prefix_cnt, col_amt_ms, col_cnt_ms):
        this_cnt = latest[col_prefix_cnt]
        last_cnt = previous[col_prefix_cnt]
        cnt_pct = ((this_cnt - last_cnt) / last_cnt) * 100 if last_cnt else 0

        this_amt = latest[col_prefix_amt]
        last_amt = previous[col_prefix_amt]
        amt_pct = ((this_amt - last_amt) / last_amt) * 100 if last_amt else 0

        this_cnt_ms = latest[col_cnt_ms]
        last_cnt_ms = previous[col_cnt_ms]

        this_amt_ms = latest[col_amt_ms]
        last_amt_ms = previous[col_amt_ms]

        return (
            f"{company}:\n"
            f"- 이번 주 주문수: {this_cnt:,.0f}건\n"
            f"- 전주 주문수: {last_cnt:,.0f}건\n"
            f"- 주문수 증감률: {cnt_pct:.2f}%\n"
            f"- 이번 주 매출: {this_amt:,.0f}원\n"
            f"- 전주 매출: {last_amt:,.0f}원\n"
            f"- 매출 증감률: {amt_pct:.2f}%\n"
            f"- 이번 주 주문수 점유율: {this_cnt_ms}%\n"
            f"- 이번 주 매출액 점유율: {this_amt_ms}%\n"
            f"- 지난 주 주문수 점유율: {last_cnt_ms}%\n"
            f"- 지난 주 매출액 점유율: {last_amt_ms}%\n"
        )

    fact_b = generate_company_facts("BDMJ", "AMT_BDMJ", "CNT_BDMJ", "SHARE_AMT_BDMJ", "SHARE_CNT_BDMJ")
    fact_c = generate_company_facts("CE", "AMT_CE", "CNT_CE", "SHARE_AMT_CE", "SHARE_CNT_CE")
    fact_y = generate_company_facts("YGY", "AMT_YGY", "CNT_YGY", "SHARE_AMT_YGY", "SHARE_CNT_YGY")

    return latest, [fact_b, fact_c, fact_y]


def get_days_raw_df():
    query_text = """
    SELECT FORMAT_DATE('%G-W%V', PARSE_DATE('%Y%m%d', A.APV_TS_D)) AS WEEK
        , PARSE_DATE('%Y%m%d', A.APV_TS_D) AS DATE
        , CASE WHEN B.KOR_DOW_CD='월요일' THEN '01_월'
                  WHEN B.KOR_DOW_CD='화요일' THEN '02_화'
                  WHEN B.KOR_DOW_CD='수요일' THEN '03_수'
                  WHEN B.KOR_DOW_CD='목요일' THEN '04_목'
                  WHEN B.KOR_DOW_CD='금요일' THEN '05_금'
                  WHEN B.KOR_DOW_CD='토요일' THEN '06_토'
                  WHEN B.KOR_DOW_CD='일요일' THEN '07_일'
              END AS DOW_KOR
        , CASE WHEN A.BRD_NM='C사' THEN 'CE'
                WHEN A.BRD_NM='Y사' THEN 'YGY' ELSE 'BDMJ' END as BRD_NM
        , A.EST_HGA AS GMV
        , A.EST_CNT AS CNT
        , SUM(A.EST_HGA) OVER (PARTITION BY A.APV_TS_D) AS SUM_GMV_DAY
        , SUM(A.EST_CNT) OVER (PARTITION BY A.APV_TS_D) AS SUM_ORDER_DAY
        , SAFE_DIVIDE(A.EST_HGA, SUM(A.EST_HGA) OVER (PARTITION BY A.APV_TS_D))*100 AS GMV_MS
        , SAFE_DIVIDE(A.EST_CNT, SUM(A.EST_CNT) OVER (PARTITION BY A.APV_TS_D))*100 AS ORDER_MS
    FROM `ygy-datawarehouse.auth_view.avw_external_shinhan_monitoring_daily` A
    LEFT JOIN `ygy-datawarehouse.mart.dim_date` B ON PARSE_DATE('%Y%m%d', CAST(A.APV_TS_D AS STRING))=B.DATE
    WHERE 1=1 
    AND PARSE_DATE('%Y%m%d', A.APV_TS_D) BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE('Asia/Seoul'))+5,7)+1 DAY), INTERVAL 12 week)+1 AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE('Asia/Seoul'))+5,7)+1 DAY)
    ORDER BY 1,2
    """
    query_job = query_reader("")
    query_job.read_query_text(query_text, query_raw=True)
    ms_df = query_job.to_pandas()
    return ms_df

def prepare_weekday_comparison(day_level_df: pd.DataFrame, target_week: str) -> pd.DataFrame:
    # 기준 주차와 전주 필터링
    week_list = sorted(day_level_df["WEEK"].unique())
    if target_week not in week_list:
        raise ValueError(f"{target_week}은 day_level_df에 존재하지 않는 주차입니다.")
    
    target_idx = week_list.index(target_week)
    if target_idx == 0:
        raise ValueError("첫 번째 주차는 비교할 전주가 없습니다.")
    
    prev_week = week_list[target_idx - 1]

    # 필터링
    df = day_level_df[day_level_df["WEEK"].isin([target_week, prev_week])].copy()

    # 비교를 위한 컬럼만 추출
    df = df[["WEEK", "DOW_KOR", "BRD_NM", "GMV", "CNT", "GMV_MS", "ORDER_MS"]]

    # 피벗: WEEK → 행 정렬
    pivoted = df.pivot_table(
        index=["DOW_KOR", "BRD_NM"],
        columns="WEEK",
        values=["GMV", "CNT", "GMV_MS", "ORDER_MS"]
    )

    # 컬럼 정리
    pivoted.columns = ['_'.join(col).strip() for col in pivoted.columns.values]
    pivoted = pivoted.reset_index()

    return pivoted

# 요약 텍스트 생성 함수 (기업 기준으로 요일별 점유율 + 변화)
def get_weekday_diff_summary(pivoted_df: pd.DataFrame, target_week: str) -> str:
    # 직전 주차 구하기
    prev_week = sorted([col.split("_")[-1] for col in pivoted_df.columns if target_week not in col and "GMV_" in col])[0]

    summary_lines = []
    brd_order = ['BDMJ', 'CE', 'YGY']
    dow_order = ['01_월', '02_화', '03_수', '04_목', '05_금', '06_토', '07_일']

    for brd in brd_order:
        brd_df = pivoted_df[pivoted_df["BRD_NM"] == brd]
        if brd_df.empty:
            continue

        summary_lines.append(f"###  {brd}")
        summary_lines.append("| 요일 | GMV 점유율(%) | 주문 점유율(%) | GMV 점유율 변화(%p) | 주문 점유율 변화(%p) |")
        summary_lines.append("|------|----------------|------------------|-----------------------|-------------------------|")

        for dow in dow_order:
            row = brd_df[brd_df["DOW_KOR"] == dow]
            if row.empty:
                continue
            row = row.squeeze()

            try:
                gmv_ms_now = row[f"GMV_MS_{target_week}"]
                gmv_ms_prev = row[f"GMV_MS_{prev_week}"]
                order_ms_now = row[f"ORDER_MS_{target_week}"]
                order_ms_prev = row[f"ORDER_MS_{prev_week}"]

                gmv_ms_diff = gmv_ms_now - gmv_ms_prev
                order_ms_diff = order_ms_now - order_ms_prev

                summary_lines.append(
                    f"| {dow.replace('_', '')} | {gmv_ms_now:.2f} | {order_ms_now:.2f} | {gmv_ms_diff:+.2f} | {order_ms_diff:+.2f} |"
                )
            except KeyError:
                continue

        summary_lines.append("")  # 줄바꿈

    return "\n".join(summary_lines)