FROM python:3.8.1-alpine3.11

ENV AWS_DEFAULT_REGION='us-west-2'
ENV BUCKET="test-cloudtrail"
ENV LOG_LOCATION="AWSLogs"
ENV ACCOUNT_ID="123456789101"
ENV DATABASE="default"
ENV TABLE_NAME="cloudtrail_logs"
ENV OUTPUT_LOC="s3://foo/bar/"

RUN apk add --update build-base bash libffi-dev openssl-dev && pip install pipenv setuptools wheel twine

RUN mkdir app

COPY Pipfile Pipfile.lock ./
RUN pipenv lock --dev -r > requirements.txt
RUN pipenv lock -r >> requirements.txt
RUN pip install -r requirements.txt

COPY . ./app

WORKDIR app
