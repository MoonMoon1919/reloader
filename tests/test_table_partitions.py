"""Module for testing events."""

# Import from std lib
import sys
from os import path
import json

# Import external libs
import botocore
import pytest

from reloader.main import TablePartitions, Athena

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def retrieve_exec_resp_fixture():
    """Load execution response test fixture data."""
    j = json.load(open("./tests/fixtures/start_query_resp.json"))
    return j


def retrieve_partition_resp_fixture():
    """Load partition response test fixture data."""
    j = json.load(open("./tests/fixtures/partitions_list_result.json"))
    return j


def retrieve_expected_partions_tree():
    """Load parition tree from fixture data."""
    j = json.load(open("./tests/fixtures/partition_map.json"))
    return j


# Monkeypatch func for execute_query func to just respond with fixture
def mock_execute_query(*args, **kwargs):
    return retrieve_exec_resp_fixture()


# Monkeypatch func for wait_for_complete func to just respond "SUCCEEDED"
def mock_wait_for_completion(*args, **kwargs):
    return "SUCCEEDED"


# Monkeypatch func for results func to just respond with fixture
def mock_results(*args, **kwargs):
    return retrieve_partition_resp_fixture()


def test_get_partitions(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "results", mock_results)
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartitions(
        athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"}
    )

    # Assert!
    assert table_client.partitions == retrieve_expected_partions_tree()


def test_query_builder(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "results", mock_results)
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartitions(
        athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"}
    )

    query_string = table_client._build_add_partition_query(
        bucket_loc="s3://foo/bar", new_partition=["us-west-2", "2020", "03", "01"]
    )

    assert (
        query_string
        == "ALTER TABLE foo ADD PARTITION (region='us-west-2',year='2020',month='03',day='01') LOCATION 's3://s3://foo/bar/us-west-2/2020/03/01/'"
    )


def test_check_partition(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "results", mock_results)
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    def mock_partitions(*args, **kwargs):
        return retrieve_expected_partions_tree()

    monkeypatch.setattr(TablePartitions, "_get_partitions", mock_partitions)

    # Create table_client
    table_client = TablePartitions(
        athena_client, "foo", {0: "region", 1: "year", 2: "month", 3: "day"}
    )

    query_string = table_client._build_add_partition_query(
        bucket_loc="s3://foo/bar", new_partition=["us-west-2", "2020", "03", "01"]
    )

    assert table_client.check_for_partition(["us-west-2", "2020", "04", "25"]) is False
    assert table_client.check_for_partition(["us-east-1", "2020", "03", "30"]) is True
