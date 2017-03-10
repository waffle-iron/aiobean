.PHONY: clean clean-build clean-test clean-pyc
clean: clean-build clean-test clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-build:
	rm -fr aiobean.egg-info

clean-test:
	rm -f .coverage
	rm -fr htmlcov

install aiobean.egg-info: clean
	pip install -Ue .[yml]

install-dev: aiobean.egg-info
	pip install -r requirements/dev.txt

install-test: aiobean.egg-info
	pip install -r requirements/test.txt

lint:
	flake8 aiobean tests

test .coverage:
	pytest

cov: .coverage
	@coverage report --skip-covered

htmlcov: .coverage
	@coverage html
	@echo "open htmlcov/index.html"

