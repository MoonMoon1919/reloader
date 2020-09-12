"""Module for testing events."""

# Import from std lib
import sys
from os import path
import json

# Import external libs
import pytest

from reloader.main import TablePartitions, Athena, ExecutionResponse

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def retrieve_exec_resp_fixture():
    """Load execution response test fixture data."""
    j = json.load(open("./tests/fixtures/execution_resp_obj.json"))
    return j


def retrieve_partition_found_resp_fixture():
    """Load partition response test fixture data."""
    j = json.load(open("./tests/fixtures/partition_found_result.json"))
    return j


def retrieve_partition_not_found_resp_fixture():
    """Load parition tree from fixture data."""
    j = json.load(open("./tests/fixtures/no_partition_found_result.json"))
    return j


# Monkeypatch func for execute_query func to just respond with fixture
def mock_execute_query(*args, **kwargs):
    return retrieve_exec_resp_fixture()


# Monkeypatch func for wait_for_complete func to just respond "SUCCEEDED"
def mock_wait_for_completion(*args, **kwargs):
    return "SUCCEEDED"


# Monkeypatch fund for results when partition is found
def mock_result_found(*args, **kwargs):
    return retrieve_partition_found_resp_fixture()


# Monkeypatch fund for results when partition is found
def mock_result_not_found(*args, **kwargs):
    return retrieve_partition_not_found_resp_fixture()


def test_query_builder(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartitions(athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"})

    query_string = table_client._build_add_partition_query(
        bucket_loc="s3://foo/bar", new_partition=["us-west-2", "2020", "03", "01"]
    )

    assert (
        query_string
        == "ALTER TABLE foo ADD PARTITION (region='us-west-2',year='2020',month='03',day='01') LOCATION 's3://s3://foo/bar/us-west-2/2020/03/01/'"
    )


def test_check_partition_found(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "results", mock_result_found)
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartitions(athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"})

    assert table_client.check_for_partition(["us-west-2", "2020", "04", "12"]) is True


def test_check_partition_not_found(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "results", mock_result_not_found)
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartitions(athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"})

    assert table_client.check_for_partition(["us-west-2", "2020", "04", "25"]) is False
