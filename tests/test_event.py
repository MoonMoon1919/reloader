"""Module for testing events."""

import sys
from os import path
import json
import pytest

from reloader.main import Event

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


def retrieve_fixture():
    """Load test fixture data."""
    j = json.load(open("./tests/fixtures/crond_event.json"))
    return j


def test_event_object():
    """Loads fixture data into event."""
    data = retrieve_fixture()
    event = Event(event=data)

    assert event.event_month == 9
    assert event.event_year == 2020
