"""
Wikipedia에서 국가별 GDP 데이터를 추출, 변환, 적재(Database)하는 ETL 스크립트입니다.

이 스크립트는 다음 단계를 포함합니다:
    1. Extract: Wikipedia의 'List of countries by GDP (nominal)' 페이지에서 국가별 GDP 데이터를 추출합니다.
    2. Transform: 추출된 데이터를 정제하고, 단위를 Billion으로 변환하며, 각 국가에 대해 Region 정보를 매핑합니다.
       또한 SQLite의 TIMESTAMP 타입과 호환되는 처리 시점을 기록합니다.
    3. Load: SQLiteHandler 클래스를 사용하여 'World_Economies.db'의 'Countries_by_GDP' 테이블에 적재합니다.
       이때 id(PK), Country, Region, GDP_USD_billion, Processed_Time 컬럼을 포함합니다.
    4. Analysis: SQL 쿼리(Window Function 등)를 사용하여 100B USD 이상 국가와 Region별 Top 5 평균 GDP를 출력합니다.

추출된 GDP 데이터 개수, Region 매핑 상태, 필터링된 데이터 개수 등 각 단계의 진행 상황을 로그 파일에 기록합니다.
"""
import datetime
import json
import os
import sqlite3

import bs4
import pandas as pd
import requests

# 설정값
WIKIPEDIA_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
COUNTRY_REGION_JSON = 'Countries_Regions.json'
LOG_FILE = 'etl_project_log.txt'
DB_NAME = 'World_Economies.db'
TABLE_NAME = 'Countries_by_GDP'


class SQLiteHandler:
    """SQLite 연결과 종료를 직접 관리하는 컨텍스트 매니저 클래스입니다.
    
    Attributes:
        db_name: 연결할 데이터베이스 파일 이름.
        conn: sqlite3 연결 객체.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = None

    def __enter__(self):
        """with 문 진입 시 호출되며, DB 연결을 생성하고 반환합니다."""
        self.conn = sqlite3.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """with 문 종료 시 호출되며, 예외 발생 시 롤백 후 연결을 안전하게 닫습니다."""
        if self.conn:
            if exc_type is not None:
                self.conn.rollback()
            self.conn.close()


def log_progress(message: str) -> None:
    """ETL 프로세스의 각 단계와 시간을 로그 파일에 기록합니다.

    Args:
        message: 기록할 로그 메시지.
    """
    timestamp_format = '%Y-%B-%d-%H-%M-%S'
    now = datetime.datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp}, {message}\n")


def extract(url: str) -> pd.DataFrame:
    """웹 페이지에서 모든 국가의 GDP 원시 데이터를 추출합니다.

    이 단계에서는 필터링을 수행하지 않고 소스 테이블의 데이터를 최대한 보존합니다.
    구조적 헤더(th) 및 static-row-header는 추출 대상에서 제외합니다.

    Args:
        url: 데이터를 추출할 위키피디아 페이지 URL.

    Returns:
        추출된 국가명과 원본 GDP 텍스트를 포함하는 데이터프레임.
    """
    log_progress("Extract phase Started")

    headers = {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = bs4.BeautifulSoup(response.text, 'html.parser')

    df = pd.DataFrame(columns=["Country", "GDP_USD_million"])

    tables = soup.find_all('table', {'class': 'wikitable'})
    target_table = next(
        (t for t in tables
         if 'GDP forecast or estimate (million US$) by country' in t.text),
        None)

    if not target_table:
        log_progress("Extract phase Failed: Target table not found")
        raise ValueError("Target table not found")

    rows = target_table.find_all('tr')
    for row in rows:
        row_classes = row.get('class')
        is_static_header = isinstance(
            row_classes, list) and 'static-row-header' in row_classes
        if row.find('th') or is_static_header:
            continue

        cols = row.find_all('td')
        if len(cols) >= 3:
            country_element = cols[0].find('a')
            country = country_element.get_text(
                strip=True) if country_element else cols[0].get_text(
                    strip=True)
            gdp_raw = cols[1].get_text(strip=True)

            new_row = pd.DataFrame([{
                "Country": country,
                "GDP_USD_million": gdp_raw
            }])
            df = pd.concat([df, new_row], ignore_index=True)

    log_progress(f"Extract phase Ended: {len(df)} rows fetched")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """데이터 세척, 단위 변환 및 데이터 처리 시점을 기록합니다.

    Args:
        df: 추출된 원시 데이터프레임.

    Returns:
        Region 정보와 TIMESTAMP 객체가 포함된 정제된 데이터프레임.
    """
    log_progress("Transform phase Started")

    # 1. 처리 시점 기록 (SQLite TIMESTAMP 호환을 위해 datetime 객체 유지)
    df['Processed_Time'] = pd.to_datetime(
        datetime.datetime.now().replace(microsecond=0))

    # 2. GDP 값 정제: 주석 제거 및 숫자형 변환
    df['GDP_USD_million'] = df['GDP_USD_million'].str.replace(
        r'[\(\[].*?[\)\]]', '', regex=True).str.strip()
    df['GDP_USD_million'] = pd.to_numeric(df['GDP_USD_million'].str.replace(
        ',', ''),
                                          errors='coerce')

    # 유효하지 않은 데이터 필터링
    df = df.dropna(subset=['GDP_USD_million']).reset_index(drop=True)

    # 3. 단위 변환 (Million -> Billion)
    df['GDP_USD_billion'] = (df['GDP_USD_million'] / 1000).round(2)

    # 4. Region 매핑
    if os.path.exists(COUNTRY_REGION_JSON):
        with open(COUNTRY_REGION_JSON, 'r', encoding='utf-8') as f:
            region_map = json.load(f)
        df['Region'] = df['Country'].map(region_map).fillna('Unknown')
    else:
        df['Region'] = 'Unknown'

    log_progress("Transform phase Ended")
    return df[['Country', 'Region', 'GDP_USD_billion', 'Processed_Time']]


def load_to_db(df: pd.DataFrame, db_name: str, table_name: str) -> None:
    """변환된 데이터를 SQLite 데이터베이스에 저장합니다.

    데이터를 적재하기 전 테이블이 없는 경우 생성하며,
    적재는 Append 모드로 수행되어 이전 이력이 누적됩니다.

    Args:
        df: 변환된 데이터프레임.
        db_name: 데이터베이스 파일 이름.
        table_name: 저장할 테이블 이름.
    """
    log_progress("Load to DB Started")

    with SQLiteHandler(db_name) as conn:
        cursor = conn.cursor()
        # id를 PK로 설정하고 Processed_Time을 TIMESTAMP 타입으로 생성
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Country TEXT NOT NULL,
                Region TEXT NOT NULL,
                GDP_USD_billion REAL NOT NULL,
                Processed_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.close()

        # 데이터 적재 (Append 모드로 이력 누적)
        df.to_sql(table_name, conn, if_exists='append', index=False)

    log_progress(f"Load to DB Ended: {db_name}")


def run_sql_analysis(db_name: str, table_name: str) -> None:
    """SQL 쿼리를 사용하여 최신 데이터를 분석하고 결과를 화면에 출력합니다.

    요구사항에 따라 100B USD 이상 국가 목록과 
    Region별 Top 5 평균 GDP를 SQL Window Function 등을 활용해 구합니다.

    Args:
        db_name: 분석할 데이터베이스 이름.
        table_name: 데이터가 저장된 테이블 이름.
    """
    log_progress("Analysis phase Started")

    with SQLiteHandler(db_name) as conn:
        # 1. 최신 데이터 기준 시점 확보
        latest_time = pd.read_sql(
            f"SELECT MAX(Processed_Time) FROM {table_name}", conn).iloc[0, 0]

        print(f"\n[ 분석 기준 시점: {latest_time} ]")

        # 2. 100B USD 이상 국가 분석 (SQL)
        query_100b = f"""
            SELECT Country, Region, GDP_USD_billion 
            FROM {table_name} 
            WHERE GDP_USD_billion >= 100 AND Processed_Time = ?
            ORDER BY GDP_USD_billion DESC
        """
        high_gdp_df = pd.read_sql(query_100b, conn, params=(latest_time, ))

        print("\n" + "=" * 70)
        print(f"{'Country':<25} {'Region':<20} {'GDP (Billion USD)':>20}")
        print("-" * 70)
        for _, row in high_gdp_df.iterrows():
            print(
                f"{row['Country']:<25} {row['Region']:<20} {row['GDP_USD_billion']:>20,.2f}"
            )
        print("-" * 70)
        print(f"총 {len(high_gdp_df)}개 국가가 100B USD 이상입니다.")

        # 3. Region별 Top 5 평균 GDP 분석 (SQL - Window Function)
        query_top5_avg = f"""
            WITH RankedGDP AS (
                SELECT Region, GDP_USD_billion, 
                       ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) as rank
                FROM {table_name} 
                WHERE Processed_Time = ?
            )
            SELECT Region, ROUND(AVG(GDP_USD_billion), 2) as Average_GDP
            FROM RankedGDP 
            WHERE rank <= 5 
            GROUP BY Region 
            ORDER BY Average_GDP DESC
        """
        region_avg_df = pd.read_sql(query_top5_avg,
                                    conn,
                                    params=(latest_time, ))

        print("\n" + "=" * 70)
        print(f"{'Region':<45} {'Top 5 Average GDP':>20}")
        print("-" * 70)
        for _, row in region_avg_df.iterrows():
            print(f"{row['Region']:<45} {row['Average_GDP']:>20,.2f}")
        print("=" * 70)

    log_progress("Analysis phase Ended")


if __name__ == '__main__':
    log_progress("ETL Process Started")
    try:
        # 1. Extract
        raw_data = extract(WIKIPEDIA_URL)

        if not raw_data.empty:
            # 2. Transform
            transformed_data = transform(raw_data)

            # 3. Load
            load_to_db(transformed_data, DB_NAME, TABLE_NAME)

            # 4. Analysis
            run_sql_analysis(DB_NAME, TABLE_NAME)

            log_progress("ETL Process Completed Successfully")

    except Exception as e:
        log_progress(f"ETL Process Failed: {str(e)}")
        print(f"Error: {e}")
