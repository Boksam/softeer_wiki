# ETL 프로세스 구현하기

이 프로젝트는 위키피디아에서 국가별 GDP 데이터를 수집하여 ETL(Extract, Transform, Load) 파이프라인을 통해 처리하고, 결과를 JSON 파일 또는 SQLite 데이터베이스에 저장하는 프로젝트입니다.

## 사용 방법

### 1. 의존성 설치

프로젝트 실행에 필요한 라이브러리를 설치합니다.

```bash
pip install -r requirements.txt
```

### 2. 스크립트 실행

이 프로젝트는 두 가지 버전의 ETL 스크립트를 제공합니다.

#### A. JSON으로 저장 (`etl_project_gdp.py`)

ETL 결과를 `Countries_by_GDP.json` 파일에 저장합니다.

```bash
python etl_project_gdp.py
```

- **Extract**: 위키피디아의 GDP 데이터를 스크래핑합니다.
- **Transform**: 데이터를 정제하고, 단위를 변환하며, `Countries_Regions.json`을 참조하여 대륙 정보를 추가합니다.
- **Load**: 변환된 데이터를 `data/Countries_by_GDP.json`에 저장합니다. 기존 데이터가 있으면 새로운 데이터를 추가(Append)합니다.

#### B. SQLite로 저장 (`etl_project_gdp_with_sql.py`)

ETL 결과를 `World_Economies.db` 데이터베이스에 저장합니다.

```bash
python etl_project_gdp_with_sql.py
```

- **Extract**: 위키피디아의 GDP 데이터를 스크래핑합니다.
- **Transform**: 데이터를 정제하고, 단위를 변환하며, `Countries_Regions.json`을 참조하여 대륙 정보를 추가합니다.
- **Load**: 변환된 데이터를 `data/World_Economies.db`의 `Countries_by_GDP` 테이블에 저장합니다.

### 3. 결과 확인

- **JSON**: `Countries_by_GDP.json` 파일을 열어 저장된 데이터를 확인합니다.
- **SQLite**: `World_Economies.db` 파일을 SQLite 뷰어를 통해 열어 `Countries_by_GDP` 테이블의 데이터를 확인합니다.
- **로그**: `etl_project_log.txt` 파일을 통해 ETL 프로세스의 각 단계별 실행 기록을 확인할 수 있습니다.

## 데이터베이스 스키마

`etl_project_gdp_with_sql.py` 실행 시 생성되는 `World_Economies.db`의 `Countries_by_GDP` 테이블 스키마는 다음과 같습니다.

| 컬럼명          | 타입      | 설명                   |
| --------------- | --------- | ---------------------- |
| id              | INTEGER   | Primary Key            |
| Country         | TEXT      | 국가명                 |
| Region          | TEXT      | 대륙명                 |
| GDP_USD_billion | REAL      | GDP (단위: 십억 달러)  |
| Processed_Time  | TIMESTAMP | 데이터 처리 시각       |
