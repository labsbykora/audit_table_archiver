"""Pytest configuration and shared fixtures."""

from collections.abc import AsyncGenerator

import pytest


@pytest.fixture
async def async_fixture() -> AsyncGenerator[None, None]:
    """Example async fixture."""
    # Setup
    yield
    # Teardown
