"""
KAMIS 가격 분석 시스템
- 최저가/최고가 분석
- 이상가격(anomaly) 감지 (IQR 기반 + Z-score)
- 계절 변화 패턴 분석
- 분석 결과를 엑셀 리포트로 출력
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

import kamis_database as db


def _parse_price(val):
    """가격 문자열을 숫자로 변환. 쉼표 제거, 빈 값/'-' 은 NaN."""
    if not val or val in ("-", "", "0"):
        return np.nan
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return np.nan


# ============================================================
# 1. 최저가 / 최고가 분석
# ============================================================

def analyze_min_max(conn=None):
    """
    일별 가격(dpr1)을 기준으로 품목별 최저가/최고가/평균가를 계산.
    Returns: DataFrame with columns [productno, product_name, category_name,
             item_name, unit, price_min, price_max, price_mean, price_count]
    """
    df = db.query_daily_prices(conn=conn)
    if df.empty:
        print("[분석] 일별 데이터가 없습니다.")
        return pd.DataFrame()

    df["price"] = df["dpr1"].apply(_parse_price)
    df = df.dropna(subset=["price"])

    if df.empty:
        print("[분석] 유효한 가격 데이터가 없습니다.")
        return pd.DataFrame()

    result = (
        df.groupby(["productno", "product_name", "category_name", "item_name", "unit"])
        .agg(
            price_min=("price", "min"),
            price_max=("price", "max"),
            price_mean=("price", "mean"),
            price_count=("price", "count"),
        )
        .reset_index()
    )

    result["price_mean"] = result["price_mean"].round(0)
    result["price_range"] = result["price_max"] - result["price_min"]
    result["price_range_pct"] = (
        (result["price_range"] / result["price_mean"] * 100).round(1)
    )

    result = result.sort_values("price_range_pct", ascending=False)
    print(f"[분석] 최저가/최고가 분석 완료: {len(result)}개 품목")
    return result


# ============================================================
# 2. 이상가격 감지
# ============================================================

def detect_anomalies(method="iqr", threshold=1.5, conn=None):
    """
    이상가격 감지.

    method:
      - 'iqr': IQR(사분위범위) 기반 (threshold = IQR 배수, 기본 1.5)
      - 'zscore': Z-score 기반 (threshold = Z-score 임계값, 기본 2.0)

    Returns: DataFrame with anomaly flags
    """
    df = db.query_daily_prices(conn=conn)
    if df.empty:
        print("[분석] 일별 데이터가 없습니다.")
        return pd.DataFrame()

    df["price"] = df["dpr1"].apply(_parse_price)
    df = df.dropna(subset=["price"])

    if df.empty:
        return pd.DataFrame()

    anomalies = []

    for (pno, pname), group in df.groupby(["productno", "product_name"]):
        prices = group["price"]

        if len(prices) < 3:
            continue

        if method == "iqr":
            q1 = prices.quantile(0.25)
            q3 = prices.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr

            outliers = group[(prices < lower) | (prices > upper)].copy()
            outliers["anomaly_type"] = np.where(
                outliers["price"] < lower, "비정상_저가", "비정상_고가"
            )
            outliers["lower_bound"] = lower
            outliers["upper_bound"] = upper

        elif method == "zscore":
            mean = prices.mean()
            std = prices.std()
            if std == 0:
                continue

            z_scores = (prices - mean) / std
            mask = z_scores.abs() > threshold

            outliers = group[mask].copy()
            outliers["z_score"] = z_scores[mask]
            outliers["anomaly_type"] = np.where(
                outliers["z_score"] < 0, "비정상_저가", "비정상_고가"
            )
            outliers["lower_bound"] = mean - threshold * std
            outliers["upper_bound"] = mean + threshold * std
        else:
            continue

        if not outliers.empty:
            anomalies.append(outliers)

    if not anomalies:
        print("[분석] 이상가격이 감지되지 않았습니다.")
        return pd.DataFrame()

    result = pd.concat(anomalies, ignore_index=True)
    result = result.sort_values("price", ascending=False)

    print(f"[분석] 이상가격 감지 완료: {len(result)}건 ({method} 방식)")
    return result


# ============================================================
# 3. 계절 변화 패턴 분석
# ============================================================

def analyze_seasonal_pattern(conn=None):
    """
    월별 데이터를 이용한 계절 변화 패턴 분석.
    - 월별 평균 가격 산출
    - 계절(봄/여름/가을/겨울) 평균 비교
    - 최고가 월, 최저가 월 식별

    Returns: dict with 'monthly_avg' and 'seasonal_summary' DataFrames
    """
    df = db.query_monthly_prices(conn=conn)
    if df.empty:
        print("[분석] 월별 데이터가 없습니다.")
        return {"monthly_avg": pd.DataFrame(), "seasonal_summary": pd.DataFrame()}

    df["price_max"] = df["monthly_max"].apply(_parse_price)
    df["price_min"] = df["monthly_min"].apply(_parse_price)
    df["price_avg"] = (df["price_max"] + df["price_min"]) / 2

    df["month"] = df["yyyymm"].str[-2:].astype(int, errors="ignore")

    # 월별 품목 평균
    monthly_avg = (
        df.groupby(["productno", "month"])
        .agg(
            avg_price=("price_avg", "mean"),
            max_price=("price_max", "max"),
            min_price=("price_min", "min"),
            data_count=("price_avg", "count"),
        )
        .reset_index()
    )
    monthly_avg["avg_price"] = monthly_avg["avg_price"].round(0)

    # 계절 매핑
    def to_season(m):
        if m in (3, 4, 5):
            return "봄"
        elif m in (6, 7, 8):
            return "여름"
        elif m in (9, 10, 11):
            return "가을"
        else:
            return "겨울"

    monthly_avg["season"] = monthly_avg["month"].apply(to_season)

    # 계절별 요약
    seasonal_summary = (
        monthly_avg.groupby(["productno", "season"])
        .agg(
            season_avg=("avg_price", "mean"),
            season_max=("max_price", "max"),
            season_min=("min_price", "min"),
        )
        .reset_index()
    )
    seasonal_summary["season_avg"] = seasonal_summary["season_avg"].round(0)

    # 품목별 최고가월/최저가월
    peak_months = (
        monthly_avg.loc[monthly_avg.groupby("productno")["avg_price"].idxmax()]
        [["productno", "month", "avg_price"]]
        .rename(columns={"month": "peak_month", "avg_price": "peak_price"})
    )

    low_months = (
        monthly_avg.loc[monthly_avg.groupby("productno")["avg_price"].idxmin()]
        [["productno", "month", "avg_price"]]
        .rename(columns={"month": "low_month", "avg_price": "low_price"})
    )

    peak_low = peak_months.merge(low_months, on="productno")
    peak_low["seasonal_gap_pct"] = (
        ((peak_low["peak_price"] - peak_low["low_price"]) / peak_low["low_price"] * 100)
        .round(1)
    )

    print(f"[분석] 계절 패턴 분석 완료: {len(monthly_avg)}행 (월별), {len(seasonal_summary)}행 (계절별)")

    return {
        "monthly_avg": monthly_avg,
        "seasonal_summary": seasonal_summary,
        "peak_low": peak_low,
    }


# ============================================================
# 4. 연도별 가격 추세 분석
# ============================================================

def analyze_yearly_trend(conn=None):
    """
    연도별 가격 추세 분석.
    - 연간 가격 변동률 계산
    - 상승/하락 추세 판별

    Returns: DataFrame with yearly trend info
    """
    df = db.query_yearly_prices(conn=conn)
    if df.empty:
        print("[분석] 연별 데이터가 없습니다.")
        return pd.DataFrame()

    df["price_max"] = df["yearly_max"].apply(_parse_price)
    df["price_min"] = df["yearly_min"].apply(_parse_price)
    df["price_avg"] = (df["price_max"] + df["price_min"]) / 2

    df = df.sort_values(["productno", "yyyy"])

    # 전년 대비 변동률
    df["prev_avg"] = df.groupby("productno")["price_avg"].shift(1)
    df["yoy_change"] = ((df["price_avg"] - df["prev_avg"]) / df["prev_avg"] * 100).round(1)

    # 추세 판별
    def calc_trend(group):
        valid = group.dropna(subset=["price_avg"])
        if len(valid) < 2:
            return "데이터부족"
        first = valid["price_avg"].iloc[0]
        last = valid["price_avg"].iloc[-1]
        if last > first * 1.1:
            return "상승추세"
        elif last < first * 0.9:
            return "하락추세"
        else:
            return "보합"

    trends = df.groupby("productno").apply(calc_trend, include_groups=False).reset_index()
    trends.columns = ["productno", "trend"]

    result = df.merge(trends, on="productno")
    print(f"[분석] 연도별 추세 분석 완료: {len(result)}행")
    return result


# ============================================================
# 5. 통합 분석 리포트 생성
# ============================================================

def generate_report(output_path=None, conn=None):
    """
    모든 분석 결과를 엑셀 리포트로 출력.
    """
    if output_path is None:
        output_path = os.path.join(
            "output", f"kamis_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    close_conn = False
    if conn is None:
        conn = db.get_connection()
        close_conn = True

    print("\n" + "=" * 60)
    print("분석 리포트 생성 시작")
    print("=" * 60)

    # 1. 최저가/최고가
    print("\n[1/4] 최저가/최고가 분석...")
    min_max = analyze_min_max(conn=conn)

    # 2. 이상가격 감지
    print("\n[2/4] 이상가격 감지 (IQR)...")
    anomalies_iqr = detect_anomalies(method="iqr", threshold=1.5, conn=conn)

    print("\n[2/4] 이상가격 감지 (Z-score)...")
    anomalies_z = detect_anomalies(method="zscore", threshold=2.0, conn=conn)

    # 3. 계절 패턴
    print("\n[3/4] 계절 패턴 분석...")
    seasonal = analyze_seasonal_pattern(conn=conn)

    # 4. 연도별 추세
    print("\n[4/4] 연도별 추세 분석...")
    yearly_trend = analyze_yearly_trend(conn=conn)

    # 엑셀 저장
    print(f"\n리포트 저장: {output_path}")
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            if not min_max.empty:
                min_max.to_excel(writer, sheet_name="최저가_최고가", index=False)

            if not anomalies_iqr.empty:
                cols = ["productno", "product_name", "category_name", "item_name",
                        "price", "anomaly_type", "lower_bound", "upper_bound",
                        "lastest_day"]
                existing = [c for c in cols if c in anomalies_iqr.columns]
                anomalies_iqr[existing].to_excel(writer, sheet_name="이상가격_IQR", index=False)

            if not anomalies_z.empty:
                cols = ["productno", "product_name", "category_name", "item_name",
                        "price", "anomaly_type", "z_score", "lower_bound", "upper_bound",
                        "lastest_day"]
                existing = [c for c in cols if c in anomalies_z.columns]
                anomalies_z[existing].to_excel(writer, sheet_name="이상가격_Zscore", index=False)

            if not seasonal["monthly_avg"].empty:
                seasonal["monthly_avg"].to_excel(writer, sheet_name="월별평균", index=False)

            if not seasonal["seasonal_summary"].empty:
                seasonal["seasonal_summary"].to_excel(writer, sheet_name="계절별요약", index=False)

            if not seasonal["peak_low"].empty:
                seasonal["peak_low"].to_excel(writer, sheet_name="최고가월_최저가월", index=False)

            if not yearly_trend.empty:
                yearly_trend.to_excel(writer, sheet_name="연도별추세", index=False)

            # DB 통계
            stats = db.get_db_stats(conn=conn)
            products = db.get_all_products(conn=conn)

            summary_data = {
                "항목": [
                    "분석일시",
                    "일별 데이터 수",
                    "월별 데이터 수",
                    "연별 데이터 수",
                    "등록 품목 수",
                    "이상가격(IQR) 건수",
                    "이상가격(Z-score) 건수",
                ],
                "값": [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    stats.get("daily_prices", 0),
                    stats.get("monthly_prices", 0),
                    stats.get("yearly_prices", 0),
                    len(products),
                    len(anomalies_iqr),
                    len(anomalies_z),
                ],
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name="분석요약", index=False)

            if not products.empty:
                products.to_excel(writer, sheet_name="품목목록", index=False)

        print(f"  리포트 저장 완료: {output_path}")

    except Exception as e:
        print(f"  리포트 저장 오류: {e}")
        output_path = None

    if close_conn:
        conn.close()

    return output_path
