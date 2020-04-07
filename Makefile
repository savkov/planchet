.PHONY: all test clean build
NAME=planchet
VERSION=$(shell git rev-parse HEAD)
SEMVER_VERSION=$(shell git describe --abbrev=0 --tags)
REPO=quay.io/savkov

clean:
	rm -rf .cache
	find . -name "*pyc" -delete
	find . -name ".coverage" -delete

login:
	docker login -u ${DOCKER_LOGIN} -p ${DOCKER_PASSWORD} quay.io

build:
	docker build -t $(REPO)/$(NAME):$(VERSION) .

install-redis:
	docker run -d -p 6379:6379 -v data:/data --name redis quay.io/savkov/redis redis-server /etc/redis/redis.conf --requirepass ${PLANCHET_REDIS_PWD}

run:
	uvicorn app:app --reload --host 0.0.0.0 --port 5005 --workers 1

run-docker:
	docker-compose up

install:
	pip install -r requirements.txt

install-test:
	pip install -r test_requirements.txt

test:
	pytest -v -m "not local"

test-coverage:
	pytest -v -m "not local" --cov-config .coveragerc --cov .
	coverage xml

test-docker:
	docker-compose -f docker-compose-test.yml build test && \
	docker-compose -f docker-compose-test.yml run test

lint:
	flake8

release:
	python setup.py sdist bdist_wheel &&\
	python -m twine upload dist/*

tag-latest:
	docker tag $(REPO)/$(NAME):$(VERSION) $(REPO)/$(NAME):latest

tag-semver:
	docker tag $(REPO)/$(NAME):$(VERSION) $(REPO)/$(NAME):$(SEMVER_VERSION)

push:
	docker push $(REPO)/$(NAME):$(SEMVER_VERSION); \
	docker push $(REPO)/$(NAME):latest;
