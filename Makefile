clean:
	-rm -r aiobean.egg-info

aiobean.egg-info:
	pip install -Ue .

devel:
	pip install -U pip
	pip install -r requirements-dev.txt

test:
	pytest

cov coverage:
	pytest --cov --cov-report=term --cov-report=html
	@echo "open htmlcov/index.html to view coverage report in html"

testing:
	ptw

