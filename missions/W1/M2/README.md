### W1M2: SQL Tutorial

- SELECT TOP 문법은 처음 봤다. MSSQL에서 사용하는 문법인 것 같다.
  - MySQL과 PostgreSQL에서 TOP에 대응되는 문법은 LIMIT이고, 이것만 사용해왔어서 새로웠다.
- SQL에서 ALIAS를 지정할 때 [] 대괄호로 묶어주면 공백이 포함된 이름이나 예약어를 사용할 수 있다는 것을 알게 되었다.
- SQL에서 LIKE를 사용할 때 기본적으로 ASCII 문자에 대해서 대소문자를 구분하지 않는다는 것을 알게 되었다.
  - SQLITE에서는 `PRAGMA case_sensitive_like` 명령어를 사용하여 대소문자 구분 여부를 설정할 수 있다.
  - 하지만 이 설정은 DEPRECATED 되었다.
  - 대소문자를 구분하고 싶다면 `GLOB` 연산자를 사용해서 패턴을 검색할 수 있다.
  - [SQLITE 공식 문서: case_sensitive_like](https://sqlite.org/pragma.html#pragma_case_sensitive_like)
  - [대소문자 구분 관련 Stack Overflow 글](https://stackoverflow.com/questions/15480319/case-sensitive-and-insensitive-like-in-sqlite)
- 데이터베이스마다 와일드카드 문자가 다를 수 있으니 조심해야 한다.
  - 예를 들어 `[bsp]`는 MSSQL에서 b, s, p 문자 중 하나를 의미하지만, SQLITE에서는 대괄호가 와일드카드로 인식되지 않는다.
  - `[a-f]`와 같은 범위 지정도 SQLITE에서는 지원되지 않는다.
- `IN`을 사용하여 여러 값 중 하나와 일치하는지 확인할 때, 여러 값은 `()` 소괄호로 묶어주어야 한다.
- 데이터베이스마다 날짜를 다루는 방식이 다르다.
  - `#01-01-01#` 형식은 MS Access에서 사용하는 날짜 리터럴이고, SQLITE에서는 인식되지 않는다.
  - SQLITE에서는 'YYYY-MM-DD' 형식의 문자열을 사용하거나, `DATE()` 함수를 사용하여 날짜를 다룬다.
- Python에서 `sqlite3`를 사용해 SELECT 쿼리를 실행했을 때, 컬럼 이름도 함께 출력하고 싶다면 `cursor.description` 속성을 활용해야 한다.
  - 예시
    ```python
    import sqlite3

    rows = cursor.execute("SELECT * FROM table_name").fetchall()

    column_names = [description[0] for description in cursor.description]
    
    results = [dict(zip(column_names, row)) for row in rows]

    print(results)
    ```
- Self Join은 같은 테이블을 두 번 참조하여 조인하는 방법이다.
  - 예를 들어, 직원 테이블에서 각 직원의 매니저 정보를 가져올 때 사용할 수 있다.
  - 주로 계층 구조 데이터를 다룰 때 사용된다.
  - 이때 테이블에 별칭(Alias)을 지정하여 구분한다.
  - 예시
    ```sql
    SELECT e1.EmployeeName AS Employee, e2.EmployeeName AS Manager
    FROM Employees e1
    LEFT JOIN Employees e2 ON e1.ManagerID = e2.EmployeeID;
    ```
- Not Equal에는 `<>`와 `!=` 두 가지 연산자가 있다.
  - 다만, `<>`가 ISO 표준이다.
- `UNION`은 두 개 이상의 SELECT 쿼리 결과를 합치는 데 사용된다.
  - `UNION`은 중복된 행을 제거하고, `UNION ALL`은 중복된 행도 모두 포함한다.
  - `UNION`을 사용할 때는 각 SELECT 문의 컬럼 수와 데이터 타입이 일치해야 한다.
  - 예시
    ```sql
    SELECT column_name FROM table1
    UNION
    SELECT column_name FROM table2;
    ```
- SQLITE는 `ANY`, `ALL` 연산자를 지원하지 않는다.
  - 대신 `IN` 연산자를 사용하여 비슷한 기능을 구현할 수 있다.


