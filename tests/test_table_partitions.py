"""Module for testing events."""

# Import from std lib
import sys
from os import path
import json

# Import external libs
import pytest

from reloader.main import TablePartition, Athena, ExecutionResponse

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def retrieve_exec_resp_fixture():
    """Load execution response test fixture data."""
    j = json.load(open("./tests/fixtures/execution_resp_obj.json"))
    return j


# Monkeypatch func for execute_query func to just respond with fixture
def mock_execute_query(*args, **kwargs):
    return retrieve_exec_resp_fixture()


# Monkeypatch func for wait_for_complete func to just respond "SUCCEEDED"
def mock_wait_for_completion(*args, **kwargs):
    return "SUCCEEDED"


def test_query_builder_add(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartition(athena_client, "foo")

    query_string = table_client._build_partition_query(
        bucket_loc="foo/bar",
        partition={"region": "us-west-2", "year": "2020", "month": "03", "day": "01"},
        action_string="ADD",
    )

    assert (
        query_string
        == "ALTER TABLE foo ADD IF NOT EXISTS PARTITION (region='us-west-2',year='2020',month='03',day='01') LOCATION 's3://foo/bar/us-west-2/2020/03/01/'"
    )


def test_query_builder_drop(monkeypatch):
    # Set attributes
    monkeypatch.setattr(Athena, "wait_for_completion", mock_wait_for_completion)
    monkeypatch.setattr(Athena, "execute_query", mock_execute_query)

    # Create athena_client
    athena_client = Athena(database="test", output_loc="s3://test/foo/bar")

    # Create table_client
    table_client = TablePartition(athena_client, "foo")

    query_string = table_client._build_partition_query(
        bucket_loc="foo/bar",
        partition={"region": "us-west-2", "year": "2020", "month": "03", "day": "01"},
        action_string="DROP",
    )

    assert (
        query_string
        == "ALTER TABLE foo DROP IF EXISTS PARTITION (region='us-west-2',year='2020',month='03',day='01')"
    )
