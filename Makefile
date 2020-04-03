.PHONY: all test clean build
NAME=planchet
VERSION=$(shell git rev-parse HEAD)
SEMVER_VERSION=$(shell git describe --abbrev=0 --tags)
REPO=quay.io/savkov

clean:
	rm -rf .cache
	find . -name "*pyc" -delete
	find . -name ".coverage" -delete

build:
	docker build -t $(REPO)/$(NAME):$(VERSION) .

install-redis:
	docker run -d -p 6379:6379 -v data:/data --name redis quay.io/savkov/redis redis-server /etc/redis/redis.conf --requirepass ${PLANCHET_REDIS_PWD}

run:
	uvicorn app:app --reload --host 0.0.0.0 --port 5005 --workers 1

install:
	pip install -r requirements.txt

install-test:
	pip install -r test_requirements.txt

test:
	pip install -r test_requirements.txt && pytest -v -m "not local" --cov-config .coveragerc --cov .
	coverage xml

test-docker:
	docker-compose run test

test-local:
	pip install -r test_requirements.txt && pytest

lint:
	pip install flake8 && flake8

release:
	python -c "text=open('setup.py').read();import re;v=re.search('version=\'([\d.vab]+)\'',text).group(1);print(v,end='')" | xargs -I{} git tag {} &&\
	pip install twine wheel &&\
	python setup.py sdist bdist_wheel &&\
	twine upload dist/*

tag-latest:
	docker tag $(REPO)/$(NAME):$(VERSION) $(REPO)/$(NAME):latest

tag-semver:
	@if docker run -e DOCKER_REPO=savkov/$(NAME) -e DOCKER_TAG=$(SEMVER_VERSION) quay.io/savkov/tag-exists; \
	    then echo "Tag $(SEMVER_VERSION) already exists!" && exit 1 ; \
	else \
			docker tag $(REPO)/$(NAME):$(VERSION) $(REPO)/$(NAME):$(SEMVER_VERSION); \
			docker push $(REPO)/$(NAME):$(SEMVER_VERSION); \
			docker tag $(REPO)/$(NAME):$(VERSION) $(REPO)/$(NAME):master; \
			docker push $(REPO)/$(NAME):master; \
	fi
