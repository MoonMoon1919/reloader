SHELL := /bin/bash
CWD := $(shell pwd)
GIT_SHA := `git rev-parse --short HEAD`
REQ_CHECK_CMD := pipenv run pip check
SAFETY_CHECK_CMD := safety check --full-report
BLACK_SET_CMD := python3 -m black --line-length 110 .
BLACK_FORMAT_CHECK := python3 -m black --check --diff --line-length 110 .
MYPY_CHECK_CMD := mypy reloader/
LINT_COMMAND := python3 -m flake8 .
TEST_CMD := pytest tests/
DOCKER_BUILD_CMD := docker build
DOCKER_RUN_CMD := docker run
IMAGE_NAME := reloader_test

sha:
	@echo $(GIT_SHA)

format:
	@$(BLACK_SET_CMD)

lint:
	@$(LINT_COMMAND)

check/format:
	@$(BLACK_FORMAT_CHECK)

check/reqs:
	@$(REQ_CHECK_CMD)

check/safety:
	@$(SAFETY_CHECK_CMD)

check/typing:
	@$(MYPY_CHECK_CMD)

test:
	@$(TEST_CMD)

build/test/image:
	@$(DOCKER_BUILD_CMD) . -t $(IMAGE_NAME):$(GIT_SHA)

run/test/image: build/test/image
	@$(DOCKER_RUN_CMD) $(IMAGE_NAME):$(GIT_SHA) make test

run/safety/image: build/test/image
	@$(DOCKER_RUN_CMD) $(IMAGE_NAME):$(GIT_SHA) make check/safety
