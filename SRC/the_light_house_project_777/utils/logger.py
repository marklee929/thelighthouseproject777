import logging
import sys
import os

# 로그 디렉터리 경로를 구성하고, 없는 경우 생성합니다.
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
APP_LOG_FILE = os.path.join(LOGS_DIR, 'app.log')

# 애플리케이션 전역 로거 설정
# 모듈 로드 시점에 한 번만 호출되어 애플리케이션 전체에 적용됩니다.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(APP_LOG_FILE, encoding='utf-8'), # 파일 핸들러 추가
        logging.StreamHandler(sys.stdout) # 기존의 콘솔 출력 유지
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    지정된 이름으로 표준 설정이 적용된 로거를 반환합니다.
    
    Args:
        name (str): 로거의 이름 (일반적으로 __name__을 사용).

    Returns:
        logging.Logger: 설정된 로거 인스턴스.
    """
    return logging.getLogger(name)