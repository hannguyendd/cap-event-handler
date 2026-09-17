from datetime import timedelta

import pytest

from cap_event_handler.config import Settings, SnsSqsConfig
from cap_event_handler.errors import CapError, ConfigurationError


def test_settings_defaults_match_spec() -> None:
    s = Settings()
    assert s.inbox_retention == timedelta(days=15)
    assert s.outbox_retention == timedelta(days=1)
    assert s.group_concurrency == 1
    assert s.consumer_tasks == 1
    assert s.max_retries == 50
    assert s.sweeper_interval == timedelta(seconds=30)
    assert s.sweeper_lookback == timedelta(seconds=60)
    assert s.collector_interval == timedelta(seconds=300)
    assert s.collector_batch == 1000
    assert s.reject_backoff_seconds == 5
    assert s.publish_timeout == timedelta(seconds=10)
    assert s.wait_seconds == 20
    assert s.max_messages == 10
    assert s.on_unroutable == "reject"
    assert s.provision is True
    assert s.auto_create_tables is True
    assert s.shutdown_timeout == timedelta(seconds=30)
    assert s.send_queue_maxsize == 1000
    assert s.schema_name == "cap"
    assert s.table_prefix == "cap_"


def test_settings_is_frozen() -> None:
    s = Settings()
    with pytest.raises(Exception):  # noqa: B017
        s.max_retries = 3  # type: ignore[misc]


def test_on_unroutable_rejects_unknown_value() -> None:
    with pytest.raises(ConfigurationError, match="on_unroutable"):
        Settings(on_unroutable="explode")  # type: ignore[arg-type]


def test_max_messages_must_be_within_sqs_limits() -> None:
    with pytest.raises(ConfigurationError, match="max_messages"):
        Settings(max_messages=11)


def test_wait_seconds_must_be_within_sqs_limits() -> None:
    with pytest.raises(ConfigurationError, match="wait_seconds"):
        Settings(wait_seconds=21)


def test_sns_sqs_config_requires_region() -> None:
    cfg = SnsSqsConfig(region="ap-southeast-2")
    assert cfg.region == "ap-southeast-2"
    assert cfg.endpoint_url is None


def test_configuration_error_is_a_cap_error() -> None:
    assert issubclass(ConfigurationError, CapError)
