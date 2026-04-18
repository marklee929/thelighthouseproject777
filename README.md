# Lighthouse Project 777

Lighthouse Project 777은 Christian news를 수집하고, 사람이 검토 가능한 Facebook posting candidate로 선별하는 standalone content pipeline 프로젝트입니다.

## 현재 목표

Phase 1은 완전 자동 발행이 아니라 운영자 중심 선별 시스템 구축에 집중합니다.

- RSS 기반 Christian news 수집
- 기사 링크, 메타데이터, 원문 저장
- 로컬 LLM 기반 기사 분석 및 점수화
- Telegram 리뷰어 검토
- 확인된 기사만 Facebook posting candidate 큐로 전환

## 핵심 원칙

- database-first
- modular service separation
- traceable ingestion flow
- human review flow
- no monolithic service files

## 현재 파이프라인

`source -> rss_feed -> article -> review -> generated_content`

## 디렉터리 안내

- `DOC/`: 아키텍처 규칙, 운영 문서, 외부 참고 자료
- `SRC/`: 애플리케이션 코드와 PostgreSQL DDL

## 시작 지점

- 프로젝트 문서 개요: `DOC/README.md`
- 소스 구조 개요: `SRC/README.md`
- 아키텍처 규칙: `DOC/architect/README.md`

## Phase 1 selection focus

기사 선별은 단순 자극성 최적화가 아니라 아래 기준을 우선합니다.

- curiosity-first framing 가능성
- psychologically safe content transformation 가능성
- PLD compatibility
- operator control and traceability
