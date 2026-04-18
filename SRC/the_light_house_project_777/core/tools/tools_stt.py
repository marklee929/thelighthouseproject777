def listen_once() -> str:
	# Minimal implementation: use keyboard input for now.
	try:
		return input("Question/command: ").strip()
	except EOFError:
		return ""


__all__ = ["listen_once"]

