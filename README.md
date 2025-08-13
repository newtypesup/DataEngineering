# Airflow & AWS Glue 기반 데이터 엔지니어링 파이프라인

## 📌 개요
이 프로젝트는 **Airflow, AWS Glue, Athena, PySpark**를 활용한 **배치 중심 데이터 파이프라인** 구현 과제입니다.  
이전 근무 환경에서 경험한 일부 프로세스를 확장하여, 스스로 기획·구현했습니다.  
원하는 형태의 데이터가 없어 **더미 데이터를 직접 설계**했고, API 형태로 실시간 적재처럼 동작하도록 **Airflow DAG**를 작성했습니다.

**데이터 흐름**
1. **React + FastAPI**로 입력받은 데이터를 **Kafka**를 통해 **PostgreSQL**에 저장  
2. **더미 데이터 생성 DAG**가 정해진 스케줄에 맞춰 ETL 수행  
3. S3에 적재된 원천 데이터를 **Airflow DAG**로 각 주제별 **Glue Job** 실행  
4. Glue Job을 통해 CSV와 Parquet 포맷으로 S3에 저장

---

## 🚀 주요 기능
- React + FastAPI를 통한 데이터 입력  
- Kafka + PostgreSQL 데이터 저장  
- Airflow DAG 기반 더미 데이터 생성  
- Airflow DAG를 통한 AWS Glue Job 실행 및 스케줄링

---

## 📚 AWS Glue Job 구성
| Job 이름                | 처리 방식                     | 출력 포맷 |
|-------------------------|-------------------------------|-----------|
| `daily_cate_top5`       | Athena 쿼리                   | CSV       |
| `weekly_cate_top5`      | Athena 쿼리                   | CSV       |
| `weekly_agerated_top5`  | Athena 쿼리                   | CSV       |
| `realtime_applist`      | PySpark + Athena 쿼리         | Parquet   |

---

## 🔍 특이 사항
- **S3 파티션 구조**: `year=YYYY/month=MM/day=DD/` 방식으로 Athena 쿼리 최적화  
- **Slack 알림**: Airflow DAG, Glue Job 성공/실패 상태 모니터링

---

## 🛠 기술 스택
- **백엔드**: Python, FastAPI, PostgreSQL, Airflow, PySpark, AWS Glue, Athena  
- **프론트엔드**: React  
- **인프라**: Docker, AWS S3  
- **기타**: Slack API (알림)

---

## 📜 프로젝트 구조

- **airflow/**: DAG, 스크립트, Slack 알림 유틸 포함  
- **backend/**: PostgreSQL 셋팅 및 Kafka 연동 파일  
- **dummy/**: 로컬 테스트용 더미 데이터 배치  
- **frontend/**: React 페이지(.jsx/.css) 및 아이콘  
- **glue_libs/**: Glue Job용 모듈(.zip)  
- **glue_scripts/**: 주제별 4개 Glue Script  
- **images/**: 과제 진행 과정 기록  
- **result_sample/**: Glue Job 결과물 샘플  
- **slack/**: 과금 및 Glue Job 실패 알림 스크립트  
- **backup_docker.ps1**: Docker 백업 스크립트  
- **restore_docker.ps1**: Docker 복원 스크립트  
- **volume_cleaner.ps1**: Docker Volumes 정리 스크립트  
- **docker-compose.yml**: Docker 설정 파일  
- **insert.sql**: 과제용 Query 모음  
- **requirements.txt**: pip 패키지 목록

---
