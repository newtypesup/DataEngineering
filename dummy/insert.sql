--테이블 설정
create table df_input(ctnt_id bigint generated always as identity (start with 100000 increment by 1), ctnt_name varchar(255), cate_name varchar(255), age_ratings varchar(255), reg_date date default current_date, primary key(ctnt_id));
create table df_category(cate_id int, parent_id int, cate_name varchar(255), age_ratings varchar(255), uid varchar(255), run_time date);
create table df_content(ctnt_id bigint, cate_id int, ctnt_name varchar(255), reg_date date, uid varchar(255), run_time date);
create table df_download(ctnt_id bigint, cnty_cd varchar(255), status varchar(255), date date, uid varchar(255), run_time date);

--드랍
drop table df_input, df_category, df_content, df_download;

--사전 데이터 입력
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



--주간 게임 연령등급별 다운 실패 탑5 (전체이용가 제외)
WITH recent_failures AS (
    SELECT
        b.ctnt_id,
        a.age_ratings,
        COUNT(*) AS fail_count
    FROM df_download AS c
    JOIN df_content AS b ON c.ctnt_id = b.ctnt_id AND c.uid = b.uid
    JOIN df_category AS a ON b.cate_id = a.cate_id AND b.uid = a.uid
    WHERE c.status = 'FAIL'
      AND a.parent_id = '0'
      AND c.date = '2025-07-01'
      --AND c.date BETWEEN '$start_date' AND '$end_date'
    GROUP BY b.ctnt_id, a.age_ratings
),

ranked_failures AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY age_ratings ORDER BY fail_count DESC) AS rnk
    FROM recent_failures
)

SELECT
	DISTINCT
    rf.ctnt_id,
    rf.age_ratings,
    rf.fail_count,
    b.ctnt_name
FROM ranked_failures rf
JOIN df_content b ON rf.ctnt_id = b.ctnt_id
WHERE age_ratings != '전체이용가' AND rnk <= 5
ORDER BY age_ratings, fail_count DESC;


--일간 다운로드 탑5 한국제외

WITH daily_top5_nk AS (
    SELECT 
        b.ctnt_id,
        b.ctnt_name,
        c.date,
        COUNT(b.ctnt_id) AS app_count,
        ROW_NUMBER() OVER (PARTITION BY c.date ORDER BY COUNT(b.ctnt_id) DESC) AS app_rank
    FROM df_content b
    JOIN df_download c ON b.uid = c.uid
    WHERE c.cnty_cd != 'KOR'
      AND c.status = 'SUCCESS'
    GROUP BY b.ctnt_id, b.ctnt_name, c.date
)

SELECT
    dt5.app_rank,
    dt5.ctnt_name,
    dt5.app_count
FROM daily_top5_nk dt5
WHERE dt5.app_rank <= 5
  AND dt5.date = '2025-07-01';
  --AND dt5.date = '$date';

--일간 다운로드 탑5 한국만

WITH daily_top5_k AS (
    SELECT 
        b.ctnt_id,
        b.ctnt_name,
        c.date,
        COUNT(b.ctnt_id) AS app_count,
        ROW_NUMBER() OVER (PARTITION BY c.date ORDER BY COUNT(b.ctnt_id) DESC) AS app_rank
    FROM df_content b
    JOIN df_download c ON b.uid = c.uid
    WHERE c.cnty_cd = 'KOR'
      AND c.status = 'SUCCESS'
    GROUP BY b.ctnt_id, b.ctnt_name, c.date
)

SELECT
    dt5.app_rank,
    dt5.ctnt_name,
    dt5.app_count
FROM daily_top5_nk dt5
WHERE dt5.app_rank <= 5
  AND dt5.date = '2025-07-01';
  --AND dt5.date = '$date';
