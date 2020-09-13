import sys
from os import path
import json
import pytest

import boto3
from botocore.stub import Stubber

from reloader.main import S3Helper

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

client = boto3.client("s3")
stubber = Stubber(client)


def retrieve_prefix_fixture():
    """Load test fixture data."""
    j = json.load(open("./tests/fixtures/s3_prefix_list.json"))
    return j


def retrieve_single_rule_fixture():
    """Load test fixture data."""
    j = json.load(open("./tests/fixtures/s3_lifecycle_single_rule.json"))
    return j


def retrieve_multi_rule_fixture():
    """Load test fixture data."""
    j = json.load(open("./tests/fixtures/s3_lifecycle_multi_rule.json"))
    return j


def test_retrieve_regions():
    """Test that regions property match what is returned from s3 list objects."""
    list_objects_response = retrieve_prefix_fixture()
    expected_params = {"Bucket": "cloudtrail", "Delimiter": "/", "Prefix": "AWSLogs/123456789012/CloudTrail/"}
    stubber.add_response("list_objects", list_objects_response, expected_params)

    helper = S3Helper(bucket="cloudtrail", account_id="123456789012")
    helper._client = client

    with stubber:
        expected_regions = [
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-south-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ca-central-1",
            "eu-central-1",
            "eu-north-1",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "sa-east-1",
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
        ]

        assert helper.regions == expected_regions


def test_lifecycle_with_single_rule_policy():
    """Test that expiration is parsed from a bucket with a single rule."""
    fixture = retrieve_single_rule_fixture()
    expected_params = {"Bucket": "cloudtrail"}
    stubber.add_response("get_bucket_lifecycle_configuration", fixture, expected_params)

    helper = S3Helper(bucket="cloudtrail", account_id="123456789012")
    helper._client = client

    with stubber:
        assert helper.experation_after_days == 90


def test_lifecycle_with_multi_rule_policy():
    """Test that expiration is parsed from a bucket with multiple rules."""
    fixture = retrieve_single_rule_fixture()
    expected_params = {"Bucket": "cloudtrail"}
    stubber.add_response("get_bucket_lifecycle_configuration", fixture, expected_params)

    helper = S3Helper(bucket="cloudtrail", account_id="123456789012")
    helper._client = client

    with stubber:
        assert helper.experation_after_days == 90
