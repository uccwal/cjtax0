import requests
import pymysql
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB 정보 (GitHub Actions 환경 변수로 지정 가능)
DB_HOST = "your_mysql_host"
DB_USER = "your_mysql_user"
DB_PASSWORD = "your_mysql_password"
DB_NAME = "your_mysql_db"

BASE_URL = "http://www.bizinfo.go.kr/uss/rss/bizinfoApi.do"
CERT_KEY = "86r0Kl"
PAGE_SIZE = 10

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def save_to_db(items):
    conn = get_connection()
    cursor = conn.cursor()
    saved_count = 0

    for item in items:
        try:
            cursor.execute("""
                INSERT IGNORE INTO policy_funds (
                    pblancId, pblancNm, pblancUrl, jrsdInsttNm, excInsttNm,
                    bsnsSumryCn, creatPnttm, reqstBeginEndDe, pldirSportRealmLclasCodeNm,
                    trgetNm, hashTags, inqireCo, totCnt
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """, (
                item.get('pblancId'),
                item.get('pblancNm'),
                item.get('pblancUrl'),
                item.get('jrsdInsttNm'),
                item.get('excInsttNm'),
                item.get('bsnsSumryCn'),
                item.get('creatPnttm'),
                item.get('reqstBeginEndDe'),
                item.get('pldirSportRealmLclasCodeNm'),
                item.get('trgetNm'),
                item.get('hashTags'),
                item.get('inqireCo', 0),
                item.get('totCnt', 0)
            ))
            if cursor.rowcount > 0:
                saved_count += 1
        except Exception as e:
            logger.error(f"DB 저장 오류: {e}")

    conn.commit()
    conn.close()
    return saved_count

def fetch_all():
    page = 1
    total_collected = 0
    total_saved = 0

    while True:
        params = {
            'crtfcKey': CERT_KEY,
            'dataType': 'json',
            'pageIndex': page,
            'searchLclasId': '01',
            'hashtags': '금융,서울,부산,대구,인천,광주,대전,경기,강원,충북,충남,전북,전남,경북,경남',
            'searchCnt': PAGE_SIZE
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            items = data.get('jsonArray', [])

            if not items:
                logger.info(f"페이지 {page}: 데이터 없음, 종료")
                break

            saved_count = save_to_db(items)
            total_collected += len(items)
            total_saved += saved_count
            logger.info(f"페이지 {page}: {len(items)}개 수집, {saved_count}개 저장")

            # totCnt로 마지막 페이지 판단
            if items and 'totCnt' in items[0]:
                total_count = items[0]['totCnt']
                total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
                if page >= total_pages:
                    logger.info("전체 페이지 수집 완료")
                    break

            page += 1

        except Exception as e:
            logger.error(f"페이지 {page} 수집 실패: {e}")
            break

    logger.info(f"총 수집: {total_collected}개, 총 저장: {total_saved}개")

if __name__ == "__main__":
    fetch_all()
