"""
Wikipedia에서 국가별 GDP 데이터를 추출, 변환, 적재하는 ETL 스크립트입니다.

이 스크립트는 다음 단계를 포함합니다:
    1. Extract: Wikipedia의 'List of countries by GDP (nominal)' 페이지에서 국가별 GDP 데이터를 추출합니다.
    2. Transform: 추출된 데이터를 정제하고, GDP 단위를 백만 달러에서 십억 달러로 변환하며, 각 국가에 대해 Region 정보를 매핑합니다.
       또한 'Processed_Time' 컬럼을 추가하여 데이터의 생성 시점을 기록합니다.
    3. Load: 변환된 데이터를 JSON 파일로 저장합니다. 기존 데이터가 존재할 경우 이력을 누적(Append)합니다.
    4. Analysis: 가장 최근에 처리된 데이터를 기준으로 100B USD 이상 국가 목록과 Region별 Top 5 평균 GDP를 출력합니다.

추출된 GDP 데이터 개수, Region 매핑 상태, 필터링된 데이터 개수 등 각 단계의 진행 상황을 로그 파일에 기록합니다.

NOTE:
    Country와 Region을 매핑하는 방법에 대해 여러 가지 전략을 고려했지만,
    Region 정보를 저장하는 JSON 파일을 별도로 유지하는 방식을 선택했습니다.

    Wikipedia의 GDP 페이지에서 Country-Region 매핑 정보가 없고,
    `country-converter`와 같은 라이브러리를 사용하면 추가적인 의존성이 발생하며,
    일부 국가에 대해 부정확한 매핑이 발생할 수 있을 뿐더러 내부 로직을 이해하기 어려워지기 때문입니다.

    Country가 자주 변경되거나 새로운 국가가 자주 추가되는 것이 아니므로,
    JSON 파일을 수동으로 유지하는 것이 더 안정적이고 투명한 방법이라고 판단했습니다.
"""
import datetime
import json
import os

import bs4
import pandas as pd
import requests

# 설정값
WIKIPEDIA_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
JSON_PATH = 'data/Countries_by_GDP.json'
COUNTRY_REGION_JSON = 'Countries_Regions.json'
LOG_FILE = 'data/etl_project_log.txt'


def init_data_dir():
    """데이터 디렉토리가 존재하지 않으면 생성합니다."""
    data_dirs = [JSON_PATH, LOG_FILE]
    for path in data_dirs:
        dir_name = os.path.dirname(path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


def log_progress(message: str) -> None:
    """ETL 프로세스의 각 단계와 시간을 로그 파일에 기록합니다.

    Args:
        message: 기록할 로그 메시지.
    """
    timestamp_format = '%Y-%B-%d-%H-%M-%S'
    now = datetime.datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(LOG_FILE, 'a+', encoding='utf-8') as f:
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
    target_table = None
    for table in tables:
        if 'GDP forecast or estimate (million US$) by country' in table.text:
            target_table = table
            break

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
        처리 시점(Processed_Time)이 포함된 정제된 데이터프레임.
    """
    log_progress("Transform phase Started")

    # 1. 처리 시점 기록
    now = datetime.datetime.now()
    df['Processed_Time'] = now.strftime('%Y-%m-%d %H:%M:%S')

    # 2. GDP 값 정제: '18,080 (2024)' 등 괄호 주석 제거 및 쉼표 제거
    # 정규 표현식을 사용하여 '(' 또는 '['로 시작하는 모든 뒤쪽 텍스트를 제거합니다.
    df['GDP_USD_million'] = df['GDP_USD_million'].str.replace(
        r'[\(\[].*?[\)\]]', '', regex=True).str.strip()
    df['GDP_USD_million'] = df['GDP_USD_million'].str.replace(',', '')

    # 숫자형 변환 (숫자가 아닌 값은 NaN 처리)
    df['GDP_USD_million'] = pd.to_numeric(df['GDP_USD_million'],
                                          errors='coerce')

    # 유효하지 않은 데이터 필터링
    initial_count = len(df)
    df = df.dropna(subset=['GDP_USD_million']).reset_index(drop=True)
    filtered_count = initial_count - len(df)

    if filtered_count > 0:
        log_progress(
            f"Transform: Filtered out {filtered_count} rows with invalid GDP data"
        )

    # 3. 단위 변환 및 Region 매핑
    df['GDP_USD_billion'] = (df['GDP_USD_million'] / 1000).round(2)

    if os.path.exists(COUNTRY_REGION_JSON):
        with open(COUNTRY_REGION_JSON, 'r', encoding='utf-8') as f:
            region_map = json.load(f)
        df['Region'] = df['Country'].map(region_map).fillna('Unknown')
    else:
        df['Region'] = 'Unknown'

    # 분석을 위한 정렬
    df = df.sort_values(by='GDP_USD_billion',
                        ascending=False).reset_index(drop=True)

    log_progress("Transform phase Ended")
    return df


def load(df: pd.DataFrame, path: str):
    """변환된 데이터를 JSON 파일로 저장합니다.

    Args:
        df: 이번 회차에 변환된 데이터프레임.
        path: 저장할 JSON 파일 경로.
    """
    log_progress("Load phase Started")

    if os.path.exists(path):
        try:
            existing_df = pd.read_json(path)
            df = pd.concat([existing_df, df], ignore_index=True)
            log_progress(
                f"Load: New records appended to existing data in {path}")
        except Exception as e:
            log_progress(
                f"Load Warning: Failed to load history, starting new file. {e}"
            )

    df.to_json(path, orient='records', indent=4, force_ascii=False)
    log_progress(f"Load phase Ended: Total {len(df)} records stored")


def run_analysis(df: pd.DataFrame):
    """최신 타임스탬프 데이터를 기준으로 분석 결과를 출력합니다.

    Args:
        df: 전체 이력이 포함된 데이터프레임.
    """
    log_progress("Analysis phase Started")

    # 최신 데이터만 추출하여 분석 수행
    latest_time = df['Processed_Time'].max()
    latest_df = df[df['Processed_Time'] == latest_time].copy()

    # 1. 100B USD 이상 국가 필터링
    high_gdp_df = latest_df[latest_df['GDP_USD_billion'] >= 100][[
        'Country', 'Region', 'GDP_USD_billion'
    ]].copy()
    high_gdp_df.index = pd.Index(range(1, len(high_gdp_df) + 1))

    print(f"\n[ 분석 기준 시점: {latest_time} ]")
    print("=" * 75)
    print(
        f"{'No.':<5} {'Country':<25} {'Region':<20} {'GDP (Billion USD)':>15}")
    print("-" * 75)
    for idx, row in high_gdp_df.iterrows():
        print(
            f"{idx:<5} {row['Country']:<25} {row['Region']:<20} {row['GDP_USD_billion']:>15,.2f}"
        )
    print("-" * 75)
    print(f"총 {len(high_gdp_df)}개 국가가 100B USD 이상입니다.")
    print("=" * 75)

    # 2. Region별 Top 5 평균
    print("\n" + "=" * 75)
    print("2. 각 Region별 Top 5 국가의 GDP 평균 (Billion USD)")
    print("-" * 75)
    region_avg_series = latest_df.groupby('Region')['GDP_USD_billion'].apply(
        lambda x: x.nlargest(5).mean())
    region_avg = region_avg_series.reset_index(
        name='Top5_Average_GDP').sort_values(by='Top5_Average_GDP',
                                             ascending=False)
    region_avg.index = pd.Index(range(1, len(region_avg) + 1))

    print(f"{'No.':<5} {'Region':<25} {'Average GDP':>15}")
    print("-" * 75)
    for idx, row in region_avg.iterrows():
        print(
            f"{idx:<5} {row['Region']:<25} {row['Top5_Average_GDP']:>15,.2f}")
    print("=" * 75)

    log_progress("Analysis phase Ended")


if __name__ == '__main__':
    # data 디렉토리 초기화
    init_data_dir()

    log_progress("ETL Process Started")
    try:
        # 1. Extract
        raw_data = extract(WIKIPEDIA_URL)

        if not raw_data.empty:
            # 2. Transform
            transformed_data = transform(raw_data)

            # 3. Load (JSON)
            load(transformed_data, JSON_PATH)

            # 4. Analysis
            if os.path.exists(JSON_PATH):
                history_df = pd.read_json(JSON_PATH)
                run_analysis(history_df)

            log_progress("ETL Process Completed Successfully")
    except Exception as e:
        log_progress(f"ETL Process Failed: {str(e)}")
        print(f"Error: {e}")
