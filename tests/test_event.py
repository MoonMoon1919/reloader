"""Module for testing events."""

import sys
from os import path
import json
import pytest

from reloader.main import Event

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def retrieve_fixture():
    """Load test fixture data."""
    j = json.load(open("./tests/fixtures/put_event.json"))
    return j


def test_event_object():
    """Loads fixture data into event."""
    data = retrieve_fixture()
    event = Event(data["Records"][0])

    assert event.s3.bucket.name == "test-cloudtrail"
    assert event.s3.object.eTag == "abcdefghijklmnopqrstuvwxyz12345675"
