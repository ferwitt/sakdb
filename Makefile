all:
	black .
	flake8 .
	mypy ./data_storage.py --ignore-missing-imports --strict
