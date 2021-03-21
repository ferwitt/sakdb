all: isort autoflake black flake8 mypy pytest

isort:
	isort sakdb todolist_example.py

autoflake:
	autoflake -i -r --expand-star-imports --remove-duplicate-keys --remove-unused-variables --verbose sakdb todolist_example.py

black:
	black sakdb todolist_example.py

flake8:
	flake8 sakdb todolist_example.py

mypy:
	mypy sakdb todolist_example.py --ignore-missing-imports --strict

pytest:
	pytest sakdb/tests/*.py
