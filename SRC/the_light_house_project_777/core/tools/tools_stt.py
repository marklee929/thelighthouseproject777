def listen_once() -> str:
	# 최소 구현: 키보드 입력으로 대체 (추후 STT 연동 가능)
	try:
		return input("질문/명령: ").strip()
	except EOFError:
		return ""


__all__ = ["listen_once"]

