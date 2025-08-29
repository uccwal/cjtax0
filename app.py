from flask import Flask, jsonify, request
import requests
import pymysql
import json
from datetime import datetime
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)


class PolicyFundCollector:
    def __init__(self, host="chunjitax.mysql.pythonanywhere-services.com", user="chunjitax", password="tax0820!!", database="chunjitax$default"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.base_url = "https://www.bizinfo.go.kr/uss/rss/bizinfoApi.do"
        self.cert_key = "86r0Kl"
        self.page_size = 10

    def get_connection(self):
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def init_database(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {self.database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.commit()
            conn.close()

            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS policy_funds
                           (
                               id
                               INT
                               AUTO_INCREMENT
                               PRIMARY
                               KEY,
                               pblancId
                               VARCHAR
                           (
                               100
                           ) UNIQUE,
                               pblancNm TEXT,
                               pblancUrl TEXT,
                               jrsdInsttNm VARCHAR
                           (
                               200
                           ),
                               excInsttNm VARCHAR
                           (
                               200
                           ),
                               bsnsSumryCn TEXT,
                               creatPnttm VARCHAR
                           (
                               20
                           ),
                               reqstBeginEndDe VARCHAR
                           (
                               50
                           ),
                               pldirSportRealmLclasCodeNm VARCHAR
                           (
                               100
                           ),
                               trgetNm TEXT,
                               hashTags TEXT,
                               inqireCo INT,
                               totCnt INT,
                               collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               INDEX idx_pblancId
                           (
                               pblancId
                           ),
                               INDEX idx_creatPnttm
                           (
                               creatPnttm
                           ),
                               INDEX idx_collected_at
                           (
                               collected_at
                           ),
                               INDEX idx_jrsdInsttNm
                           (
                               jrsdInsttNm
                           ),
                               FULLTEXT idx_search
                           (
                               pblancNm,
                               bsnsSumryCn,
                               trgetNm
                           )
                               ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE =utf8mb4_unicode_ci
                           ''')

            conn.commit()
            conn.close()
            logger.info("데이터베이스 초기화 완료")
        except Exception as e:
            logger.error(f"데이터베이스 초기화 오류: {e}")
            raise

    def fetch_page_data(self, page_index=1):
        params = {
            'crtfcKey': self.cert_key,
            'dataType': 'json',
            'pageIndex': page_index,
            'searchLclasId': '01',
            'hashtags': '금융,서울,부산,대구,인천,광주,대전,경기,강원,충북,충남,전북,전남,경북,경남',
            'searchCnt': self.page_size
        }

        url = f"{self.base_url}?" + "&".join([f"{k}={v}" for k, v in params.items()])
        logger.info(f"API 호출 URL: {url}")

        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()

            logger.info(f"응답 상태코드: {response.status_code}")
            response_text = response.text
            logger.info(f"응답 내용 (처음 1000자): {response_text[:1000]}")

            data = response.json()
            logger.info(f"파싱된 JSON 구조: {list(data.keys())}")

            return {'url': url, 'data': data, 'raw_response': response_text}
        except requests.exceptions.RequestException as e:
            logger.error(f"페이지 {page_index} 데이터 수집 실패: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류 (페이지 {page_index}): {e}")
            return None

    def save_to_database(self, items):
        conn = self.get_connection()
        cursor = conn.cursor()

        saved_count = 0
        for item in items:
            try:
                cursor.execute('''
                               INSERT
                               IGNORE INTO policy_funds (
                    pblancId, pblancNm, pblancUrl, jrsdInsttNm, excInsttNm,
                    bsnsSumryCn, creatPnttm, reqstBeginEndDe, pldirSportRealmLclasCodeNm,
                    trgetNm, hashTags, inqireCo, totCnt
                ) VALUES (
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s,
                               %s
                               )
                               ''', (
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

            except pymysql.IntegrityError:
                pass
            except Exception as e:
                logger.error(f"데이터 저장 오류: {e}")

        conn.commit()
        conn.close()
        return saved_count

    def collect_data(self, max_pages=None):
        total_collected = 0
        total_saved = 0
        page = 1
        api_calls = []
        debug_info = []

        logger.info("데이터 수집 시작")

        while True:
            if max_pages and page > max_pages:
                break

            result = self.fetch_page_data(page)
            if not result:
                logger.warning(f"페이지 {page} 데이터 수집 실패, 중단")
                break

            api_calls.append(result['url'])
            data = result['data']

            items = []
            debug_info.append(f"페이지 {page} - 응답 키들: {list(data.keys())}")

            if 'jsonArray' in data:
                json_array = data['jsonArray']
                debug_info.append(f"페이지 {page} - jsonArray 타입: {type(json_array)}")

                if isinstance(json_array, list):
                    items = json_array
                    debug_info.append(f"페이지 {page} - jsonArray 리스트에서 {len(items)}개 아이템 추출")
                elif isinstance(json_array, dict):
                    debug_info.append(f"페이지 {page} - jsonArray 키들: {list(json_array.keys())}")
                    if 'item' in json_array:
                        items = json_array['item'] if isinstance(json_array['item'], list) else [json_array['item']]
            elif 'list' in data and isinstance(data['list'], list):
                items = data['list']

            debug_info.append(f"페이지 {page} - 최종 추출된 아이템 수: {len(items)}")

            if not items:
                debug_info.append(f"페이지 {page}: 더 이상 데이터가 없습니다")
                if page == 1:
                    debug_info.append(f"첫 페이지 응답 전체 구조: {json.dumps(data, ensure_ascii=False, indent=2)[:2000]}")
                break

            if page == 1 and items:
                debug_info.append(f"첫 번째 아이템 구조: {list(items[0].keys())}")

            saved_count = self.save_to_database(items)
            total_collected += len(items)
            total_saved += saved_count

            logger.info(f"페이지 {page}: {len(items)}개 수집, {saved_count}개 새로 저장")

            if items and 'totCnt' in items[0]:
                total_count = items[0]['totCnt']
                total_pages = (total_count + self.page_size - 1) // self.page_size
                if page >= total_pages:
                    logger.info(f"마지막 페이지 {page} 수집 완료")
                    break

            page += 1

        result = {
            'total_pages': page - 1,
            'total_collected': total_collected,
            'total_saved': total_saved,
            'collection_time': datetime.now().isoformat(),
            'api_calls': api_calls,
            'debug_info': debug_info
        }

        logger.info(f"수집 완료: {result}")
        return result

    def get_database_stats(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) as total_count FROM policy_funds')
        total_count = cursor.fetchone()['total_count']

        cursor.execute('SELECT COUNT(*) as today_count FROM policy_funds WHERE DATE(collected_at) = CURDATE()')
        today_count = cursor.fetchone()['today_count']

        cursor.execute(
            'SELECT MIN(creatPnttm) as min_date, MAX(creatPnttm) as max_date FROM policy_funds WHERE creatPnttm IS NOT NULL')
        date_range = cursor.fetchone()

        cursor.execute(
            'SELECT jrsdInsttNm, COUNT(*) as cnt FROM policy_funds WHERE jrsdInsttNm IS NOT NULL GROUP BY jrsdInsttNm ORDER BY cnt DESC LIMIT 5')
        top_institutions = cursor.fetchall()

        conn.close()

        return {
            'total_count': total_count,
            'today_collected': today_count,
            'date_range': (date_range['min_date'], date_range['max_date']),
            'top_institutions': [(inst['jrsdInsttNm'], inst['cnt']) for inst in top_institutions]
        }

    def search_funds(self, keyword, institution=None, limit=100):
        conn = self.get_connection()
        cursor = conn.cursor()

        query = 'SELECT * FROM policy_funds WHERE (pblancNm LIKE %s OR bsnsSumryCn LIKE %s OR trgetNm LIKE %s)'
        params = [f'%{keyword}%', f'%{keyword}%', f'%{keyword}%']

        if institution:
            query += ' AND jrsdInsttNm LIKE %s'
            params.append(f'%{institution}%')

        query += ' ORDER BY creatPnttm DESC LIMIT %s'
        params.append(limit)

        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return results

    def get_funds_paginated(self, page=1, per_page=20):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) as total FROM policy_funds')
        total = cursor.fetchone()['total']

        offset = (page - 1) * per_page
        cursor.execute('SELECT * FROM policy_funds ORDER BY creatPnttm DESC, id DESC LIMIT %s OFFSET %s',
                       (per_page, offset))
        items = cursor.fetchall()
        conn.close()

        return {
            'items': items,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        }


# 전역 collector 인스턴스
collector = PolicyFundCollector()


@app.route('/')
def index():
    html = '''<!DOCTYPE html>
<html>
<head>
    <title>정책자금 수집기</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        .btn { 
            display: inline-block; padding: 10px 20px; 
            background: #007bff; color: white; text-decoration: none; 
            border-radius: 5px; margin: 5px;
        }
        .btn:hover { background: #0056b3; }
        .btn-success { background: #28a745; }
        .btn-success:hover { background: #218838; }
        .btn-danger { background: #dc3545; }
        .btn-danger:hover { background: #c82333; }
        .stats { background: #f8f9fa; padding: 20px; margin: 20px 0; border-radius: 5px; }
        .search { margin: 20px 0; }
        .search input { padding: 8px; width: 200px; margin-right: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>정책자금 수집기</h1>

        <div class="stats">
            <h3>현재 상태</h3>
            <div id="stats">통계를 불러오는 중...</div>
        </div>

        <h3>관리 기능</h3>
        <div>
            <a href="/collect" class="btn">데이터 수집 시작</a>
            <a href="/collect?pages=5" class="btn">5페이지만 수집</a>
            <a href="/stats" class="btn">통계 보기</a>
            <a href="/manage" class="btn btn-success">데이터 관리</a>
            <a href="/debug" class="btn btn-danger">API 디버그</a>
        </div>

        <div class="search">
            <h3>검색 테스트</h3>
            <input type="text" id="keyword" placeholder="검색 키워드" value="창업">
            <button onclick="searchTest()" class="btn">검색</button>
            <div id="search-results"></div>
        </div>
    </div>

    <script>
    window.onload = function() {
        loadStats();
    };

    function loadStats() {
        fetch('/stats')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var statsDiv = document.getElementById('stats');
                if (data.error) {
                    statsDiv.innerHTML = '통계 로드 오류: ' + data.error;
                } else {
                    statsDiv.innerHTML = 
                        '총 데이터: ' + data.total_count + '개<br>' +
                        '오늘 수집: ' + data.today_collected + '개<br>' +
                        '데이터 기간: ' + data.date_range[0] + ' ~ ' + data.date_range[1] + '<br>' +
                        '주요 기관: ' + data.top_institutions.map(function(i) { return i[0]; }).join(', ');
                }
            })
            .catch(function(error) {
                document.getElementById('stats').innerHTML = '통계 로드 실패';
            });
    }

    function searchTest() {
        var keyword = document.getElementById('keyword').value;
        if (!keyword) {
            alert('검색 키워드를 입력하세요');
            return;
        }

        fetch('/search?keyword=' + encodeURIComponent(keyword) + '&limit=5')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                var resultsDiv = document.getElementById('search-results');
                if (data.error) {
                    resultsDiv.innerHTML = '<h4>검색 오류</h4><p>' + data.error + '</p>';
                } else if (data.length === 0) {
                    resultsDiv.innerHTML = '<h4>검색 결과</h4><p>결과가 없습니다.</p>';
                } else {
                    var results = data.map(function(item) {
                        return '<div style="border:1px solid #ddd; padding:10px; margin:5px;">' +
                               '<strong>' + (item.pblancNm || '제목 없음') + '</strong><br>' +
                               '<small>기관: ' + (item.jrsdInsttNm || '정보 없음') + ' | 등록: ' + (item.creatPnttm || '날짜 없음') + '</small><br>' +
                               '<a href="' + (item.pblancUrl || '#') + '" target="_blank">자세히 보기</a>' +
                               '</div>';
                    }).join('');
                    resultsDiv.innerHTML = '<h4>검색 결과 (' + data.length + '개)</h4>' + results;
                }
            })
            .catch(function(error) {
                document.getElementById('search-results').innerHTML = '<h4>검색 실패</h4><p>네트워크 오류가 발생했습니다.</p>';
            });
    }
    </script>
</body>
</html>'''
    return html


@app.route('/collect')
def collect_data():
    try:
        max_pages = request.args.get('pages', type=int)
        result = collector.collect_data(max_pages=max_pages)
        return jsonify({
            'success': True,
            'message': '데이터 수집 완료',
            'result': result
        })
    except Exception as e:
        logger.error(f"수집 오류: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/stats')
def get_stats():
    try:
        stats = collector.get_database_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"통계 조회 오류: {e}")
        return jsonify({
            'error': str(e)
        }), 500


@app.route('/search')
def search_funds():
    try:
        keyword = request.args.get('keyword', '')
        institution = request.args.get('institution', '')
        limit = request.args.get('limit', 20, type=int)

        if not keyword:
            return jsonify([])

        results = collector.search_funds(
            keyword=keyword,
            institution=institution if institution else None,
            limit=limit
        )
        return jsonify(results)
    except Exception as e:
        logger.error(f"검색 오류: {e}")
        return jsonify({
            'error': str(e)
        }), 500


@app.route('/manage')
def manage_page():
    try:
        page = request.args.get('page', 1, type=int)
        data = collector.get_funds_paginated(page=page, per_page=20)

        # 테이블 행들 생성
        table_rows = ""
        for item in data['items']:
            title = (item['pblancNm'] or '')[:50]
            table_rows += f"""
            <tr>
                <td>{item['id']}</td>
                <td>{title}</td>
                <td>{item['jrsdInsttNm'] or ''}</td>
                <td>{item['trgetNm'] or ''}</td>
                <td>{item['creatPnttm'] or ''}</td>
                <td>{item['inqireCo'] or 0}</td>
            </tr>"""

        # 페이지네이션
        pagination = ""
        if data['page'] > 1:
            pagination += f'<a href="/manage?page={data["page"] - 1}" class="btn">이전</a>'

        for p in range(max(1, data['page'] - 2), min(data['total_pages'] + 1, data['page'] + 3)):
            if p == data['page']:
                pagination += f'<span class="btn" style="background:#6c757d;">{p}</span>'
            else:
                pagination += f'<a href="/manage?page={p}" class="btn">{p}</a>'

        if data['page'] < data['total_pages']:
            pagination += f'<a href="/manage?page={data["page"] + 1}" class="btn">다음</a>'

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>정책자금 관리</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .btn {{ 
            display: inline-block; padding: 8px 16px; 
            background: #007bff; color: white; text-decoration: none; 
            border-radius: 4px; margin: 2px;
        }}
        .btn:hover {{ background: #0056b3; }}
        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        .table th, .table td {{ padding: 8px; border: 1px solid #ddd; text-align: left; }}
        .table th {{ background: #f8f9fa; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>정책자금 관리</h1>
        <p><a href="/" class="btn">메인으로</a></p>

        <p>총 {data['total']}건 | {data['page']}/{data['total_pages']} 페이지</p>

        <table class="table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>제목</th>
                    <th>기관</th>
                    <th>대상</th>
                    <th>등록일</th>
                    <th>조회수</th>
                </tr>
            </thead>
            <tbody>
                {table_rows}
            </tbody>
        </table>

        <div style="text-align: center;">
            {pagination}
        </div>
    </div>
</body>
</html>"""

        return html
    except Exception as e:
        return f"오류: {str(e)}"


@app.route('/debug')
def debug_api():
    try:
        result = collector.fetch_page_data(1)
        if result:
            return jsonify({
                'success': True,
                'url': result['url'],
                'response_keys': list(result['data'].keys()),
                'full_response': result['data'],
                'raw_response_preview': result['raw_response'][:2000]
            })
        else:
            return jsonify({
                'success': False,
                'error': 'API 호출 실패'
            }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == "__main__":
    try:
        print("정책자금 수집기 서버 시작")
        print("http://localhost:5000 에서 접속하세요")
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        print(f"서버 시작 오류: {e}")
        print("MySQL 서버가 실행 중인지 확인해주세요.")