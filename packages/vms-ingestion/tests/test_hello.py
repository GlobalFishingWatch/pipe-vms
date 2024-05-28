"""Hello unit test module."""

from vms_ingestion.hello import hello


def test_hello():
    """Test the hello function."""
    assert hello() == "Hello vms-ingestion"
