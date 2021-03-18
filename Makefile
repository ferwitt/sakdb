all:
	isort .
	autoflake -i -r --expand-star-imports  --remove-duplicate-keys --remove-unused-variables --verbose  .
	black .
	flake8 .
	mypy ./data_storage.py --ignore-missing-imports --strict
	pytest *_test.py
