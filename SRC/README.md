# Lighthouse Project 777 Source Overview

이 디렉터리는 Lighthouse Project 777의 실행 코드와 데이터베이스 구현 자산을 담고 있습니다.

## 구조 개요

- `database/`: PostgreSQL DDL 및 스키마 변경 자산
- `the_light_house_project_777/`: 실제 애플리케이션 패키지

## 현재 구현 방향

프로젝트는 standalone product-oriented content pipeline을 목표로 하며, Phase 1에서는 아래 흐름을 우선합니다.

1. RSS 소스 정의 로드
2. 기사 수집 및 정규화
3. PostgreSQL 저장
4. 로컬 LLM 기반 기사 분석
5. Telegram 리뷰어 검토
6. Facebook posting candidate 생성

## 코드 설계 원칙

- 비즈니스 로직은 서비스 계층에 둡니다.
- 외부 통신은 `integrations/` 아래에 분리합니다.
- DB 접근은 `repositories/`에 한정합니다.
- DDL과 스키마 변경 자산은 `SRC/database/` 아래에서 관리합니다.
- 오케스트레이션은 얇게 유지합니다.
- 대형 단일 서비스 파일을 만들지 않습니다.

## 주요 패키지 예시

- `database/ddl/`: phase별 PostgreSQL DDL
- `integrations/rss/`: RSS 및 원문 수집 연동
- `repositories/`: PostgreSQL persistence
- `services/ingestion/`: 기사 수집 파이프라인
- `services/analysis/`: reaction / PLD / operational scoring
- `services/review/`: Telegram 리뷰 및 결정 반영
- `services/selection/`: phase-1 기사 선별 오케스트레이션
