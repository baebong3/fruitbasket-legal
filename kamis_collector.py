"""
KAMIS API 데이터 수집기
- 일별/월별/연별 농산물 가격 데이터를 수집
- 병렬 처리로 빠르게 수집
- 누적 엑셀 저장 지원
"""

import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://www.kamis.or.kr/service/price/xml.do"

CERT_KEY = os.getenv("KAMIS_CERT_KEY", "")
CERT_ID = os.getenv("KAMIS_CERT_ID", "")

ENABLE_MONTHLY = True
ENABLE_YEARLY = True


def check_kamis_result(root, context_label=""):
    """KAMIS XML 응답의 resultCode 검사."""
    result_code = root.findtext(".//resultCode")
    result_msg = root.findtext(".//resultMsg")

    if result_code and result_code != "0000":
        print(f"[KAMIS 오류] {context_label} resultCode={result_code}, resultMsg={result_msg}")
        return False
    return True


def get_daily_sales_list(product_cls_code="01"):
    """API 6번: 최근일자 도소매가격정보 조회."""
    params = {
        "action": "dailySalesList",
        "p_cert_key": CERT_KEY,
        "p_cert_id": CERT_ID,
        "p_returntype": "xml",
        "p_product_cls_code": product_cls_code,
    }

    print(f"\n[API] dailySalesList 호출 - product_cls_code={product_cls_code}")
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        if not check_kamis_result(root, "dailySalesList"):
            return []

        data_list = []
        for item in root.findall(".//item"):
            data = {
                "product_cls_code": item.findtext("product_cls_code", ""),
                "product_cls_name": item.findtext("product_cls_name", ""),
                "category_code": item.findtext("category_code", ""),
                "category_name": item.findtext("category_name", ""),
                "productno": item.findtext("productno", ""),
                "lastest_day": item.findtext("lastest_day", ""),
                "productName": item.findtext("productName", ""),
                "item_name": item.findtext("item_name", ""),
                "unit": item.findtext("unit", ""),
                "day1": item.findtext("day1", ""),
                "dpr1": item.findtext("dpr1", ""),
                "day2": item.findtext("day2", ""),
                "dpr2": item.findtext("dpr2", ""),
                "day3": item.findtext("day3", ""),
                "dpr3": item.findtext("dpr3", ""),
                "day4": item.findtext("day4", ""),
                "dpr4": item.findtext("dpr4", ""),
                "direction": item.findtext("direction", ""),
                "value": item.findtext("value", ""),
            }
            data_list.append(data)

        print(f"  일별 데이터 수집 완료: {len(data_list)}개 품목")
        return data_list

    except Exception as e:
        print(f"  일별 데이터 수집 오류: {e}")
        return []


def get_monthly_price_trend(productno, product_cls_code="01", start_month=None, end_month=None):
    """API 8번: 월평균 가격추이 조회."""
    if not ENABLE_MONTHLY:
        return []

    now = datetime.now()
    if not start_month:
        start_month = now.strftime("%Y%m")
    if not end_month:
        end_month = now.strftime("%Y%m")

    params = {
        "action": "monthlyPriceTrendList",
        "p_cert_key": CERT_KEY,
        "p_cert_id": CERT_ID,
        "p_returntype": "xml",
        "p_productno": productno,
        "p_startmonth": start_month,
        "p_endmonth": end_month,
        "p_product_cls_code": product_cls_code,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        if not check_kamis_result(root, f"monthlyPriceTrendList(productno={productno})"):
            return []

        items = root.findall(".//item")
        if not items:
            return []

        data_list = []
        for item in items:
            yyyy = item.findtext("yyyy", "")
            mm = item.findtext("mm", "")
            yyyymm = f"{yyyy}{mm}" if yyyy and mm else ""
            data = {
                "productno": productno,
                "yyyymm": yyyymm,
                "monthly_max": item.findtext("max", ""),
                "monthly_min": item.findtext("min", ""),
            }
            data_list.append(data)
        return data_list

    except Exception as e:
        print(f"  월별 수집 오류 (품목: {productno}): {e}")
        return []


def get_yearly_price_trend(productno, product_cls_code="01", start_year=None, end_year=None):
    """API 9번: 연평균 가격추이 조회."""
    if not ENABLE_YEARLY:
        return []

    now_year = datetime.now().year
    if not start_year:
        start_year = str(now_year - 5)
    if not end_year:
        end_year = str(now_year)

    params = {
        "action": "yearlyPriceTrendList",
        "p_cert_key": CERT_KEY,
        "p_cert_id": CERT_ID,
        "p_returntype": "xml",
        "p_productno": productno,
        "p_startday": start_year,
        "p_endday": end_year,
        "p_product_cls_code": product_cls_code,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        if not check_kamis_result(root, f"yearlyPriceTrendList(productno={productno})"):
            return []

        items = root.findall(".//item")
        if not items:
            return []

        data_list = []
        for item in items:
            data = {
                "productno": productno,
                "yyyy": item.findtext("yyyy", ""),
                "yearly_max": item.findtext("max", ""),
                "yearly_min": item.findtext("min", ""),
            }
            data_list.append(data)
        return data_list

    except Exception as e:
        print(f"  연별 수집 오류 (품목: {productno}): {e}")
        return []


def collect_all_data(
    product_cls_code="01",
    start_month=None,
    end_month=None,
    start_year=None,
    end_year=None,
    max_workers=10,
):
    """모든 API 데이터를 병렬로 수집하고 통합."""
    print("=" * 60)
    print("KAMIS 데이터 수집 시작")
    print("=" * 60)

    # 1. 일별 데이터
    print("\n[1/3] 일별 가격 데이터 수집 중...")
    daily_data = get_daily_sales_list(product_cls_code)

    if not daily_data:
        print("  일별 데이터를 가져올 수 없습니다.")
        return None

    products = [
        (item["productno"], item["productName"])
        for item in daily_data
        if item.get("productno")
    ]
    products = list({p[0]: p for p in products}.values())
    print(f"  월/연 수집 대상 품목 수: {len(products)}개")

    # 2. 월별 데이터 병렬 수집
    monthly_data_all = []
    if ENABLE_MONTHLY and products:
        print(f"\n[2/3] 월별 데이터 수집 중... ({len(products)}개 품목, 병렬)")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            monthly_func = partial(
                get_monthly_price_trend,
                product_cls_code=product_cls_code,
                start_month=start_month,
                end_month=end_month,
            )
            futures = {
                executor.submit(monthly_func, pno): (pno, name)
                for pno, name in products
            }
            done = 0
            total = len(futures)
            for future in as_completed(futures):
                pno, name = futures[future]
                try:
                    monthly_data_all.extend(future.result())
                except Exception as e:
                    print(f"    월별 오류 - {name}: {e}")
                finally:
                    done += 1
                    if done % 10 == 0 or done == total:
                        print(f"    진행: {done}/{total}")
    else:
        print("\n[2/3] 월별 수집 비활성화")

    # 3. 연별 데이터 병렬 수집
    yearly_data_all = []
    if ENABLE_YEARLY and products:
        print(f"\n[3/3] 연별 데이터 수집 중... ({len(products)}개 품목, 병렬)")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            yearly_func = partial(
                get_yearly_price_trend,
                product_cls_code=product_cls_code,
                start_year=start_year,
                end_year=end_year,
            )
            futures = {
                executor.submit(yearly_func, pno): (pno, name)
                for pno, name in products
            }
            done = 0
            total = len(futures)
            for future in as_completed(futures):
                pno, name = futures[future]
                try:
                    yearly_data_all.extend(future.result())
                except Exception as e:
                    print(f"    연별 오류 - {name}: {e}")
                finally:
                    done += 1
                    if done % 10 == 0 or done == total:
                        print(f"    진행: {done}/{total}")
    else:
        print("\n[3/3] 연별 수집 비활성화")

    # 데이터프레임 변환
    df_daily = pd.DataFrame(daily_data)
    df_monthly = pd.DataFrame(monthly_data_all) if monthly_data_all else pd.DataFrame()
    df_yearly = pd.DataFrame(yearly_data_all) if yearly_data_all else pd.DataFrame()

    print(f"\n  일별: {len(df_daily)}행 | 월별: {len(df_monthly)}행 | 연별: {len(df_yearly)}행")

    return {"daily": df_daily, "monthly": df_monthly, "yearly": df_yearly}


def save_to_excel(data_dict, filename=None, cumulative=True):
    """
    수집된 데이터를 엑셀 파일로 저장.
    cumulative=True: 기존 파일이 있으면 데이터를 누적 (중복 제거).
    """
    if filename is None:
        filename = os.path.join("output", "kamis_price_data.xlsx")

    filepath = os.path.abspath(filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # 누적 모드: 기존 데이터 로드 후 합치기
    if cumulative and os.path.exists(filepath):
        print(f"\n[누적] 기존 파일에서 데이터 로드: {filepath}")
        try:
            existing = pd.read_excel(filepath, sheet_name=None, engine="openpyxl")

            for key in ["daily", "monthly", "yearly"]:
                sheet_name = {"daily": "일별가격", "monthly": "월별추이", "yearly": "연별추이"}[key]
                if sheet_name in existing and not data_dict[key].empty:
                    old_df = existing[sheet_name]
                    new_df = pd.concat([old_df, data_dict[key]], ignore_index=True)
                    new_df = new_df.drop_duplicates()
                    data_dict[key] = new_df
                    print(f"  {sheet_name}: 기존 {len(old_df)}행 + 신규 -> 누적 {len(new_df)}행")
        except Exception as e:
            print(f"  기존 파일 로드 실패, 새로 생성합니다: {e}")

    print(f"\n[저장] {filepath}")

    try:
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            if not data_dict["daily"].empty:
                data_dict["daily"].to_excel(writer, sheet_name="일별가격", index=False)
                print(f"  일별가격: {len(data_dict['daily'])}행")

            if not data_dict["monthly"].empty:
                data_dict["monthly"].to_excel(writer, sheet_name="월별추이", index=False)
                print(f"  월별추이: {len(data_dict['monthly'])}행")

            if not data_dict["yearly"].empty:
                data_dict["yearly"].to_excel(writer, sheet_name="연별추이", index=False)
                print(f"  연별추이: {len(data_dict['yearly'])}행")

            if not data_dict["daily"].empty:
                summary_cols = [
                    "productno", "productName", "category_name",
                    "item_name", "unit", "dpr1", "dpr2", "dpr3", "dpr4",
                ]
                existing_cols = [c for c in summary_cols if c in data_dict["daily"].columns]
                summary = data_dict["daily"][existing_cols].copy()
                summary.to_excel(writer, sheet_name="품목별요약", index=False)

        print(f"  파일 저장 완료: {filepath}")
        return filepath

    except Exception as e:
        print(f"  엑셀 저장 오류: {e}")
        return None
