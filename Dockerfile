FROM python:3.8.9-alpine3.13

ENV AWS_DEFAULT_REGION='us-west-2'
ENV BUCKET="test-cloudtrail"
ENV LOG_LOCATION="AWSLogs"
ENV ACCOUNT_ID="123456789101"
ENV DATABASE="default"
ENV TABLE_NAME="cloudtrail_logs"
ENV OUTPUT_LOC="s3://foo/bar/"
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

RUN apk add --update build-base bash libffi-dev openssl-dev git
RUN pip install --upgrade pip
RUN pip install pipenv setuptools wheel twine

RUN mkdir app

COPY Pipfile Pipfile.lock ./
RUN pipenv install --dev --system
RUN pipenv install --system

COPY . ./app

WORKDIR app
