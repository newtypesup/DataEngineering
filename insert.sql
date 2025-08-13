--------------------------------------------------------------------------------------------------------------------
--테이블 설정--
--------------------------------------------------------------------------------------------------------------------
create table df_input(ctnt_id bigint generated always as identity (start with 100000 increment by 1), ctnt_name varchar(255), cate_name varchar(255), age_ratings varchar(255), reg_date date default current_date, primary key(ctnt_id));
create table df_category(cate_id int, parent_id int, cate_name varchar(255), age_ratings varchar(255), uid varchar(255), run_time date);
create table df_content(ctnt_id bigint, cate_id int, ctnt_name varchar(255), reg_date date, uid varchar(255), run_time date);
create table df_download(ctnt_id bigint, cnty_cd varchar(255), status varchar(255), date date, uid varchar(255), run_time date);


--------------------------------------------------------------------------------------------------------------------
--드랍--
--------------------------------------------------------------------------------------------------------------------
drop table df_input, df_category, df_content, df_download;


--------------------------------------------------------------------------------------------------------------------
--사전 데이터 입력--
--------------------------------------------------------------------------------------------------------------------
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('카카오톡', '소셜 미디어', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('인스타그램', '소셜 미디어', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('페이스북', '소셜 미디어', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('틱톡', '소셜 미디어', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('스냅챗', '소셜 미디어', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('트위터(X)', '소셜 미디어', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('라인', '소셜 미디어', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('디스코드', '소셜 미디어', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('클럽하우스', '소셜 미디어', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('텔레그램', '소셜 미디어', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('메이플 스토리', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('배틀그라운드 모바일', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('로블록스', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('브롤스타즈', '게임', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('쿠키런 킹덤', '게임', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('모여봐요 동물의 숲', '게임', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('마인크래프트', '게임', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('클래시 로얄', '게임', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('포켓몬GO', '게임', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('피파 모바일', '게임', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('멜론', '음악', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('스포티파이', '음악', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('지니뮤직', '음악', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('벅스', '음악', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('유튜브 뮤직', '음악', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('플로', '음악', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('뮤직메이트', '음악', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('네이버 바이브', '음악', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('샤잠', '음악', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('SoundCloud', '음악', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('QR코드 스캐너', '도구', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('파일 관리자', '도구', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('날씨', '도구', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('계산기', '도구', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('메모장', '도구', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('시계', '도구', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('램정리 도우미', '도구', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('손전등', '도구', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('녹음기', '도구', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('단위 변환기', '도구', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('야놀자', '여행', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('에어비앤비', '여행', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('트리바고', '여행', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('스카이스캐너', '여행', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('카약', '여행', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Booking.com', '여행', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('호텔스닷컴', '여행', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('대한항공', '여행', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('아시아나항공', '여행', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('고속버스모바일', '여행', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('구글 문서', '오피스', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Microsoft Word', '오피스', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Microsoft Excel', '오피스', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('PDF Reader', '오피스', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('노션', '오피스', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('한컴오피스', '오피스', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Google Keep', '오피스', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Trello', '오피스', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Slack', '오피스', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Zoom', '오피스', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Zedge', '맟춤 설정', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Nova Launcher', '맟춤 설정', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Go Launcher', '맟춤 설정', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('아이콘팩', '맟춤 설정', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('폰트 변경기', '맟춤 설정', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('테마 스토어', '맟춤 설정', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('배경화면 갤러리', '맟춤 설정', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Edge Lighting', '맟춤 설정', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Always on Display 설정기', '맟춤 설정', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('위젯마법사', '맟춤 설정', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('스노우', '사진', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('B612', '사진', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('VSCO', '사진', '12세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('PicsArt', '사진', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Lightroom', '사진', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('캔바', '사진', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Photoroom', '사진', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Snapseed', '사진', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('Remini', '사진', '전체이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('YouCam Perfect', '사진', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('리그 오브 레전드', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('피파 모바일', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('클래시 오브 클랜', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('콜 오브 듀티 모바일', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('아스팔트 9', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('서머너즈 워', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('세븐나이츠', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('V4', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('이터널 리턴', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('블레이드 앤 소울 레볼루션', '게임', '15세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('배틀그라운드 모바일', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('검은사막 모바일', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('리니지M', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('리니지2M', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('오딘: 발할라 라이징', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('던전앤파이터 모바일', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('히트2', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('뮤 아크엔젤', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('킹덤: 왕가의 피', '게임', '18세이용가');
INSERT INTO df_input (ctnt_name, cate_name, age_ratings) VALUES ('아키에이지 워', '게임', '18세이용가');


--------------------------------------------------------------------------------------------------------------------
--일간 카테고리별 다운로드 탑5--
--------------------------------------------------------------------------------------------------------------------
WITH daily_cate_top5 AS (
  SELECT
    a.cate_name,
    b.ctnt_id,
    b.ctnt_name,
    c.date,
    COUNT(b.ctnt_id) AS app_count,
    ROW_NUMBER() OVER (PARTITION BY a.cate_name ORDER BY COUNT(b.ctnt_id) DESC) AS app_rank
  FROM category a
  JOIN content b ON a.uid = b.uid
  JOIN download c ON b.uid = c.uid
  WHERE c.status = 'SUCCESS'
  AND c.date = DATE '$today'
  GROUP BY a.cate_name, b.ctnt_id, b.ctnt_name, c.date
)
SELECT
  dct5.app_rank,
  dct5.cate_name,
  dct5.ctnt_name,
  dct5.app_count
FROM daily_cate_top5 dct5
WHERE dct5.app_rank <= 5;


--------------------------------------------------------------------------------------------------------------------
--주간 카테고리별 다운로드 탑5--
--------------------------------------------------------------------------------------------------------------------
WITH weekly_cate_top5 AS (
  SELECT
    a.cate_name,
    b.ctnt_id,
    b.ctnt_name,
    c.date,
    COUNT(b.ctnt_id) AS app_count,
    ROW_NUMBER() OVER (PARTITION BY a.cate_name ORDER BY COUNT(b.ctnt_id) DESC) AS app_rank
  FROM category a
  JOIN content b ON a.uid = b.uid
  JOIN download c ON b.uid = c.uid
  WHERE c.status = 'SUCCESS'
  AND c.date BETWEEN  DATE '$start_date' AND DATE '$end_date'
  GROUP BY a.cate_name, b.ctnt_id, b.ctnt_name, c.date
)
SELECT
  wct5.app_rank,
  wct5.cate_name,
  wct5.ctnt_name,
  wct5.app_count
FROM weekly_cate_top5 wct5
WHERE wct5.app_rank <= 5;


--------------------------------------------------------------------------------------------------------------------
--주간 연령 등급별 게임 다운로드 탑5 (전체이용가 제외)--
--------------------------------------------------------------------------------------------------------------------
WITH weekly_agerated_games AS (
  SELECT
    a.age_ratings,
    b.ctnt_id,
    b.ctnt_name,
    COUNT(b.ctnt_id) AS download_count
  FROM category a
  JOIN content b ON a.cate_id = b.cate_id AND a.uid = b.uid
  JOIN download c ON b.ctnt_id = c.ctnt_id AND b.uid = c.uid
  WHERE c.status = 'SUCCESS'
  AND a.parent_id = 0
  AND c.date BETWEEN DATE '$start_date' AND DATE '$end_date'
  GROUP BY a.age_ratings, b.ctnt_id, b.ctnt_name
),
weekly_game_rankings AS (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY age_ratings ORDER BY download_count DESC) AS app_rank
  FROM weekly_agerated_games
)
SELECT
  DISTINCT
  wgr.app_rank,
  wgr.age_ratings,
  wgr.ctnt_name,
  wgr.download_count
FROM weekly_game_rankings wgr
WHERE age_ratings != '전체이용가' AND app_rank <= 5
ORDER BY age_ratings ASC;


--------------------------------------------------------------------------------------------------------------------
--실시간 앱 다운로드 탑10(오리지널)--
--------------------------------------------------------------------------------------------------------------------
WITH prev_data AS (
    SELECT 
        a.cate_name,
        b.ctnt_id,
        b.ctnt_name,
        b.reg_date,
        COUNT(*) AS prev_count
    FROM content b
    JOIN category a 
        ON b.cate_id = a.cate_id AND b.uid = a.uid
    JOIN download c 
        ON b.ctnt_id = c.ctnt_id AND b.uid = c.uid
    WHERE c.status != 'FAIL'
        AND c.run_time = parse_datetime('$prev_time', '%Y%m%d%H')
    GROUP BY b.ctnt_id, b.ctnt_name, b.reg_date, a.cate_name
),
current_data AS (
    SELECT 
        a.cate_name,
        b.ctnt_id,
        b.ctnt_name,
        b.reg_date,
        COUNT(*) AS current_count
    FROM content b
    JOIN category a 
        ON b.cate_id = a.cate_id AND b.uid = a.uid
    JOIN download c 
        ON b.ctnt_id = c.ctnt_id AND b.uid = c.uid
    WHERE c.status != 'FAIL'
        AND c.run_time = parse_datetime('$current_time', '%Y%m%d%H')
    GROUP BY b.ctnt_id, b.ctnt_name, b.reg_date, a.cate_name
),
current_date AS (
    SELECT DATE('$current_date') AS crt_date
)

SELECT
    cd.ctnt_id,
    cd.ctnt_name,
    cd.cate_name,
    cd.reg_date,
    current_count,
    prev_count,
    c.crt_date,
    date_diff('day', cd.reg_date, c.crt_date) AS dates,
    SQRT(date_diff('day', cd.reg_date, c.crt_date)) AS sqrt_current_date,
    SQRT(date_diff('day', pd.reg_date, c.crt_date)) AS sqrt_prev_date,
    (current_count / NULLIF(SQRT(date_diff('day', cd.reg_date, c.crt_date)), 0)) AS current_days,
    (prev_count / NULLIF(SQRT(date_diff('day', pd.reg_date, c.crt_date)), 0)) AS prev_days,
    ROUND(
        (current_count / NULLIF(SQRT(date_diff('day', cd.reg_date, c.crt_date)), 0)) - 1, 
        2
    ) AS inc_index,
    ROUND(
        (
            (current_count / NULLIF(SQRT(date_diff('day', cd.reg_date, c.crt_date)), 0)) -
            (prev_count / NULLIF(SQRT(date_diff('day', pd.reg_date, c.crt_date)), 0))
        ), 
        2
    ) AS diff_index,
    GREATEST(
        ROUND(
            (current_count / NULLIF(SQRT(date_diff('day', cd.reg_date, c.crt_date)), 0)) - 1, 
            2
        ),
        ROUND(
            (
                (current_count / NULLIF(SQRT(date_diff('day', cd.reg_date, c.crt_date)), 0)) -
                (prev_count / NULLIF(SQRT(date_diff('day', pd.reg_date, c.crt_date)), 0))
            ), 
            2
        )
    ) AS result_index
FROM current_data cd
LEFT JOIN prev_data pd
    ON cd.ctnt_id = pd.ctnt_id
    AND cd.ctnt_name = pd.ctnt_name
CROSS JOIN current_date c
ORDER BY result_index DESC
LIMIT 10;

--------------------------------------------------------------------------------------------------------------------
--실시간 앱 다운로드 탑10(쿼리 분리 & pyspark로 적용)--
--------------------------------------------------------------------------------------------------------------------
--Previous
SELECT 
    a.cate_name,
    b.ctnt_id,
    b.ctnt_name,
    b.reg_date,
    COUNT(*) AS prev_count
FROM content b
JOIN category a ON b.cate_id = a.cate_id AND b.uid = a.uid
JOIN download c ON b.ctnt_id = c.ctnt_id AND b.uid = c.uid
WHERE c.status != 'FAIL'
  AND c.run_time = '$prev_time'
GROUP BY b.ctnt_id, b.ctnt_name, b.reg_date, a.cate_name

--current
SELECT 
    a.cate_name,
    b.ctnt_id,
    b.ctnt_name,
    b.reg_date,
    COUNT(*) AS current_count
FROM content b
JOIN category a ON b.cate_id = a.cate_id AND b.uid = a.uid
JOIN download c ON b.ctnt_id = c.ctnt_id AND b.uid = c.uid
WHERE c.status != 'FAIL'
  AND c.run_time = '$current_time'
GROUP BY b.ctnt_id, b.ctnt_name, b.reg_date, a.cate_name

--------------------------------------------------------------------------------------------------------------------