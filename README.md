# Binance Bot

바이낸스 선물 시장 데이터 수집과 전략 신호, 주문 처리 및 모니터링을 구성한 Python 프로젝트입니다. 코드에는 거래소 주문과 포지션을 처리하는 기능이 포함되어 있습니다.

## 구성

- `main.py`: 비동기 작업 및 실시간 데이터 처리 진입점
- `data_pipeline.py`, `hft_pipeline.py`: 시장 데이터 수집
- `strategy.py`, `scoring.py`, `risk_management.py`: 전략·점수·위험 관리
- `execution.py`, `database.py`: 주문 처리 및 데이터 저장
- `notification.py`, `telegram_commands.py`: 텔레그램 알림과 명령
- `dashboard/`, `dashboard.py`: 대시보드 관련 코드
- `docs/`: 아키텍처와 전략 설계 문서
- `requirements.txt`, `settings.json`: 의존성과 설정

## 사용 전 확인

실행은 설정 및 자격 증명에 따라 **실제 거래·주문 또는 외부 서비스 접근을 유발할 수 있습니다.** 환경, 거래 모드, 권한, 위험 설정과 데이터베이스 연결을 확인하세요. 이 README 작성 과정에서는 프로그램을 실행하거나 거래 모드를 검증하지 않았습니다.

전략 설명은 [전략 문서](docs/strategies/bb_squeeze_breakout.md) 및 [아키텍처 문서](docs/blueprints/architecture_blueprint.md)를 참고하세요. 문서는 실제 코드와 차이가 있을 수 있습니다. API 키나 계정 정보는 저장소에 커밋하지 마세요.
