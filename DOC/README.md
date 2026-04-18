# Lighthouse Project 777 Documents

이 디렉터리는 Lighthouse Project 777의 설계 원칙, 운영 문서, 참고 자료를 보관하는 문서 영역입니다.

## 목적

- 프로젝트의 아키텍처 규칙과 데이터베이스 설계 기준을 관리합니다.
- RSS 수집, 기사 분석, Telegram 리뷰, Facebook 후보 생성까지의 운영 흐름을 문서화합니다.
- PLD Framework, SNS 전략 자료, 뉴스 소스 수집 기준 같은 외부 참고 자료를 함께 유지합니다.

## 주요 하위 디렉터리

- `architect/`: 아키텍처 규칙, DB 기준, 설계 원칙, 버전 정책
- `documents/`: 뉴스 소스, PLD, SNS 전략 관련 참조 문서
- `images/`: 문서용 이미지 자산
- `walkthough/`: 운영 또는 구조 이해를 돕는 부가 문서

## 현재 프로젝트 범위

Phase 1은 완전 자동 발행이 아니라, 운영자 중심 기사 선별 시스템 구축에 집중합니다.

- Christian news RSS 수집
- 기사 원문과 메타데이터 저장
- 로컬 LLM 기반 기사 적합도 분석
- Telegram 리뷰어 검토
- Facebook posting candidate 큐 생성

핵심 원칙은 과도한 자동화보다 추적 가능성, 모듈 분리, 운영자 제어를 우선하는 것입니다.
