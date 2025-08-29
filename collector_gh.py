import requests
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "http://www.bizinfo.go.kr/uss/rss/bizinfoApi.do"
CERT_KEY = "86r0Kl"
PAGE_SIZE = 10  # API 기본 단위

# GitHub repository raw URL로 업로드용
OUTPUT_FILENAME = "data.json"

def fetch_page(page_index=1):
    params = {
        'crtfcKey': CERT_KEY,
        'dataType': 'json',
        'pageIndex': page_index,
        'searchLclasId': '01',
        'hashtags': '금융,서울,부산,대구,인천,광주,대전,경기,강원,충북,충남,전북,전남,경북,경남',
        'searchCnt': PAGE_SIZE
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('jsonArray', [])
    except Exception as e:
        logger.error(f"페이지 {page_index} 오류: {e}")
        return []

def collect_all():
    # 먼저 1페이지를 가져와 전체 건수 확인
    first_page = fetch_page(1)
    if not first_page:
        return []

    total_count = 9999  # 혹시 API에서 totalCount 제공하면 여기로
    all_items = first_page
    logger.info(f"페이지 1: {len(first_page)}개 수집")

    page_index = 2
    while True:
        items = fetch_page(page_index)
        if not items:
            break
        all_items.extend(items)
        logger.info(f"페이지 {page_index}: {len(items)}개 수집")
        page_index += 1

    return all_items

def save_json(data):
    with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"{len(data)}개 데이터 저장 완료 → {OUTPUT_FILENAME}")

if __name__ == "__main__":
    data = collect_all()
    save_json(data)
