import pytest
from httpx import ASGITransport, AsyncClient

from hookrelay.main import create_app


@pytest.fixture()
async def app():
    return create_app()


@pytest.fixture()
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
