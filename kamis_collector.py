"""KAMIS 농산물 유통정보 API 데이터 수집 모듈"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2


def load_kamis_config() -> dict:
    """kamis_config.json 설정 파일 로드"""
    config_path = Path(__file__).parent / "kamis_config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_kamis_api_key() -> str:
    """환경변수에서 KAMIS API 키 조회"""
    key = os.environ.get("KAMIS_API_KEY")
    if not key:
        logger.error("KAMIS_API_KEY 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    return key


def get_kamis_cert_key() -> str:
    """환경변수에서 KAMIS 인증 키 조회"""
    key = os.environ.get("KAMIS_CERT_KEY")
    if not key:
        logger.error("KAMIS_CERT_KEY 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    return key


def _request_with_retry(url: str, params: dict) -> requests.Response | None:
    """재시도 및 지수 백오프를 적용한 GET 요청"""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code >= 500 and attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning(
                    "서버 오류 %d, %d초 후 재시도 (%d/%d)",
                    resp.status_code, wait, attempt + 1, MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.ConnectionError as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("연결 오류, %d초 후 재시도 (%d/%d): %s", wait, attempt + 1, MAX_RETRIES, e)
                time.sleep(wait)
                continue
            logger.error("API 요청 실패 (최대 재시도 초과): %s", e)
            return None
        except requests.Timeout as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("타임아웃, %d초 후 재시도 (%d/%d): %s", wait, attempt + 1, MAX_RETRIES, e)
                time.sleep(wait)
                continue
            logger.error("API 요청 타임아웃 (최대 재시도 초과): %s", e)
            return None
        except requests.HTTPError as e:
            logger.error("API 요청 실패: %s", e)
            return None
    return None


def fetch_daily_price(
    api_key: str,
    cert_key: str,
    config: dict,
    item_category_code: str,
    item_code: str,
    target_date: datetime,
    kind_code: str | None = None,
    product_rank_code: str | None = None,
    country_code: str | None = None,
) -> dict | None:
    """KAMIS API에서 특정 품목의 일별 가격 데이터 조회

    Args:
        api_key: KAMIS API 키
        cert_key: KAMIS 인증 키
        config: kamis_config.json 설정
        item_category_code: 부류 코드 (100=식량작물, 200=채소류, 300=특용작물, 400=과일류)
        item_code: 품목 코드
        target_date: 조회 대상일
        kind_code: 품종 코드 (기본값: config에서 로드)
        product_rank_code: 등급 코드 (기본값: config에서 로드)
        country_code: 지역 코드 (기본값: 전국)

    Returns:
        API 응답 데이터 딕셔너리 또는 None
    """
    url = config.get("api_url", "http://www.kamis.or.kr/service/price/xml.do")
    days_back = config.get("collect_days_back", 1)

    start_date = target_date - timedelta(days=days_back)

    params = {
        "action": "dailySalesList",
        "p_cert_id": config.get("cert_id", "111"),
        "p_cert_key": cert_key,
        "p_returntype": "json",
        "p_product_cls_code": config.get("product_cls_code", "02"),
        "p_item_category_code": item_category_code,
        "p_item_code": item_code,
        "p_kind_code": kind_code or config.get("default_kind_code", "01"),
        "p_produce_rank_code": product_rank_code or config.get("default_product_rank_code", "04"),
        "p_startday": start_date.strftime("%Y-%m-%d"),
        "p_endday": target_date.strftime("%Y-%m-%d"),
        "p_convert_kg_yn": config.get("convert_kg_yn", "Y"),
        "p_country_code": country_code or config.get("country_code", ""),
    }

    # API 키는 cert_key와 별도로 p_cert_id에 매핑
    # KAMIS는 p_cert_key로 인증

    resp = _request_with_retry(url, params)
    if resp is None:
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.error("JSON 파싱 실패. 응답: %s", resp.text[:500])
        return None

    return data


def parse_price_data(raw_data: dict, item_name: str) -> list[dict]:
    """KAMIS API 응답에서 가격 데이터를 파싱하여 정규화된 리스트로 반환

    Returns:
        [{"date": "2025-01-01", "category_name": "...", "item_name": "...",
          "kind_name": "...", "rank": "...", "unit": "...", "price": 1234,
          "market_name": "..."}, ...]
    """
    results = []

    # KAMIS API 응답 구조: {"data": {"error_code": "000", "item": [...]}}
    data_section = raw_data.get("data", {})

    error_code = data_section.get("error_code", "")
    if error_code != "000":
        if error_code == "001":
            logger.debug("'%s': 데이터 없음 (error_code=001)", item_name)
        else:
            logger.warning("'%s': API 에러 코드 %s", item_name, error_code)
        return results

    items = data_section.get("item", [])
    if not items or not isinstance(items, list):
        return results

    for item in items:
        price_str = item.get("dpr1", "")  # 당일 가격
        # 가격 문자열 정리 (쉼표 제거, '-' 처리)
        price = _parse_price_string(price_str)

        results.append({
            "date": item.get("yyyy", "") + "-" + _zero_pad(item.get("regday", "")),
            "category_code": item.get("item_category_code", ""),
            "category_name": item.get("item_category_name", ""),
            "item_code": item.get("item_code", ""),
            "item_name": item.get("item_name", item_name),
            "kind_code": item.get("kind_code", ""),
            "kind_name": item.get("kind_name", ""),
            "rank": item.get("rank", ""),
            "rank_code": item.get("rank_code", ""),
            "unit": item.get("unit", ""),
            "price": price,
            "market_name": item.get("county_name", ""),
        })

    return results


def _parse_price_string(price_str: str) -> int | None:
    """가격 문자열을 정수로 변환 (쉼표 제거, '-' -> None)"""
    if not price_str or price_str.strip() in ("-", "", "0"):
        return None
    try:
        return int(price_str.replace(",", "").strip())
    except ValueError:
        return None


def _zero_pad(date_part: str) -> str:
    """'1/5' -> '01-05' 형태로 변환"""
    if not date_part:
        return ""
    parts = date_part.replace(".", "/").split("/")
    if len(parts) == 2:
        return f"{int(parts[0]):02d}-{int(parts[1]):02d}"
    return date_part


def collect_all_prices(config: dict | None = None) -> list[dict]:
    """설정된 모든 품목의 가격 데이터를 수집

    Args:
        config: kamis_config.json 설정 (None이면 파일에서 로드)

    Returns:
        정규화된 가격 데이터 리스트
    """
    if config is None:
        config = load_kamis_config()

    api_key = get_kamis_api_key()
    cert_key = get_kamis_cert_key()
    target_date = datetime.now(KST)
    all_prices = []

    categories = config.get("categories", [])
    total_items = sum(len(cat.get("item_codes", [])) for cat in categories)
    logger.info("수집 대상: %d개 부류, %d개 품목", len(categories), total_items)
    logger.info("조회 기준일: %s", target_date.strftime("%Y-%m-%d"))

    collected_count = 0
    for category in categories:
        category_code = category["category_code"]
        category_name = category["category_name"]
        item_codes = category.get("item_codes", [])

        logger.info("[%s] %s - %d개 품목 수집 시작", category_code, category_name, len(item_codes))

        for item in item_codes:
            item_code = item["code"]
            item_name = item["name"]

            raw_data = fetch_daily_price(
                api_key=api_key,
                cert_key=cert_key,
                config=config,
                item_category_code=category_code,
                item_code=item_code,
                target_date=target_date,
            )

            if raw_data is None:
                logger.warning("  '%s'(%s) 데이터 수집 실패", item_name, item_code)
                continue

            prices = parse_price_data(raw_data, item_name)
            if prices:
                all_prices.extend(prices)
                collected_count += len(prices)
                logger.info("  '%s'(%s): %d건 수집", item_name, item_code, len(prices))
            else:
                logger.debug("  '%s'(%s): 데이터 없음", item_name, item_code)

            # API 부하 방지를 위한 딜레이
            time.sleep(0.3)

    logger.info("수집 완료: 총 %d건", collected_count)
    return all_prices


if __name__ == "__main__":
    prices = collect_all_prices()
    if prices:
        logger.info("수집 데이터 샘플 (첫 3건):")
        for p in prices[:3]:
            logger.info("  %s | %s | %s | %s원", p["date"], p["item_name"], p["unit"], p["price"])
    else:
        logger.info("수집된 데이터가 없습니다.")
