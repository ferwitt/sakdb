all:
	isort sakdb
	autoflake -i -r --expand-star-imports  --remove-duplicate-keys --remove-unused-variables --verbose  sakdb
	black sakdb
	flake8 sakdb
	mypy sakdb --ignore-missing-imports --strict
	pytest sakdb/tests/*.py
