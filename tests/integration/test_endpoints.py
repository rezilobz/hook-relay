"""Integration tests for /endpoints CRUD — requires PostgreSQL (testcontainers)."""

import uuid

import pytest
from httpx import AsyncClient

AUTH = {"X-API-Key": "dev-secret"}

VALID_BODY = {
    "url": "https://example.com/webhook",
    "description": "Test endpoint",
    "event_types": ["order.created"],
    "secret": "my-signing-secret",
}


async def _create(client: AsyncClient, body: dict[str, object] | None = None) -> dict[str, object]:
    payload = VALID_BODY if body is None else body
    resp = await client.post("/endpoints", json=payload, headers=AUTH)
    assert resp.status_code == 201
    return resp.json()  # type: ignore[no-any-return]


# ─── POST /endpoints ──────────────────────────────────────────────────────────


@pytest.mark.integration
class TestCreateEndpoint:
    async def test_returns_201_with_all_fields(self, client: AsyncClient) -> None:
        resp = await client.post("/endpoints", json=VALID_BODY, headers=AUTH)

        assert resp.status_code == 201
        data = resp.json()
        assert data["url"] == "https://example.com/webhook"
        assert data["description"] == "Test endpoint"
        assert data["event_types"] == ["order.created"]
        assert data["enabled"] is True
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    async def test_secret_is_stored_and_returned(self, client: AsyncClient) -> None:
        data = await _create(client)
        assert data["secret"] == VALID_BODY["secret"]

    async def test_description_is_optional(self, client: AsyncClient) -> None:
        body = {**VALID_BODY, "description": None}
        resp = await client.post("/endpoints", json=body, headers=AUTH)
        assert resp.status_code == 201
        assert resp.json()["description"] is None

    async def test_missing_auth_header_returns_422(self, client: AsyncClient) -> None:
        resp = await client.post("/endpoints", json=VALID_BODY)
        assert resp.status_code == 422

    async def test_wrong_api_key_returns_401(self, client: AsyncClient) -> None:
        resp = await client.post("/endpoints", json=VALID_BODY, headers={"X-API-Key": "wrong"})
        assert resp.status_code == 401

    async def test_invalid_url_returns_422(self, client: AsyncClient) -> None:
        body = {**VALID_BODY, "url": "not-a-url"}
        resp = await client.post("/endpoints", json=body, headers=AUTH)
        assert resp.status_code == 422

    async def test_empty_event_types_returns_422(self, client: AsyncClient) -> None:
        body = {**VALID_BODY, "event_types": []}
        resp = await client.post("/endpoints", json=body, headers=AUTH)
        assert resp.status_code == 422

    async def test_empty_secret_returns_422(self, client: AsyncClient) -> None:
        body = {**VALID_BODY, "secret": ""}
        resp = await client.post("/endpoints", json=body, headers=AUTH)
        assert resp.status_code == 422


# ─── GET /endpoints ───────────────────────────────────────────────────────────


@pytest.mark.integration
class TestListEndpoints:
    async def test_returns_empty_list_when_none_exist(self, client: AsyncClient) -> None:
        resp = await client.get("/endpoints", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_returns_all_created_endpoints(self, client: AsyncClient) -> None:
        await _create(client, {**VALID_BODY, "url": "https://a.example.com/hook"})
        await _create(client, {**VALID_BODY, "url": "https://b.example.com/hook"})

        resp = await client.get("/endpoints", headers=AUTH)
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    async def test_missing_auth_returns_422(self, client: AsyncClient) -> None:
        resp = await client.get("/endpoints")
        assert resp.status_code == 422


# ─── GET /endpoints/{id} ──────────────────────────────────────────────────────


@pytest.mark.integration
class TestGetEndpoint:
    async def test_returns_endpoint_by_id(self, client: AsyncClient) -> None:
        created = await _create(client)

        resp = await client.get(f"/endpoints/{created['id']}", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]

    async def test_unknown_id_returns_404(self, client: AsyncClient) -> None:
        resp = await client.get(f"/endpoints/{uuid.uuid4()}", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(self, client: AsyncClient) -> None:
        created = await _create(client)
        resp = await client.get(f"/endpoints/{created['id']}")
        assert resp.status_code == 422


# ─── PATCH /endpoints/{id} ────────────────────────────────────────────────────


@pytest.mark.integration
class TestUpdateEndpoint:
    async def test_partial_update_enabled_flag(self, client: AsyncClient) -> None:
        created = await _create(client)

        resp = await client.patch(
            f"/endpoints/{created['id']}", json={"enabled": False}, headers=AUTH
        )
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    async def test_update_url(self, client: AsyncClient) -> None:
        created = await _create(client)
        new_url = "https://updated.example.com/hook"

        resp = await client.patch(
            f"/endpoints/{created['id']}", json={"url": new_url}, headers=AUTH
        )
        assert resp.status_code == 200
        assert resp.json()["url"] == new_url

    async def test_update_event_types(self, client: AsyncClient) -> None:
        created = await _create(client)

        resp = await client.patch(
            f"/endpoints/{created['id']}",
            json={"event_types": ["order.created", "order.cancelled"]},
            headers=AUTH,
        )
        assert resp.status_code == 200
        assert resp.json()["event_types"] == ["order.created", "order.cancelled"]

    async def test_unset_fields_are_unchanged(self, client: AsyncClient) -> None:
        created = await _create(client)

        resp = await client.patch(
            f"/endpoints/{created['id']}", json={"enabled": False}, headers=AUTH
        )
        data = resp.json()
        assert data["url"] == created["url"]
        assert data["event_types"] == created["event_types"]
        assert data["secret"] == created["secret"]

    async def test_unknown_id_returns_404(self, client: AsyncClient) -> None:
        resp = await client.patch(
            f"/endpoints/{uuid.uuid4()}", json={"enabled": False}, headers=AUTH
        )
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(self, client: AsyncClient) -> None:
        created = await _create(client)
        resp = await client.patch(f"/endpoints/{created['id']}", json={"enabled": False})
        assert resp.status_code == 422


# ─── DELETE /endpoints/{id} ───────────────────────────────────────────────────


@pytest.mark.integration
class TestDeleteEndpoint:
    async def test_returns_204(self, client: AsyncClient) -> None:
        created = await _create(client)

        resp = await client.delete(f"/endpoints/{created['id']}", headers=AUTH)
        assert resp.status_code == 204

    async def test_deleted_endpoint_is_gone(self, client: AsyncClient) -> None:
        created = await _create(client)
        await client.delete(f"/endpoints/{created['id']}", headers=AUTH)

        resp = await client.get(f"/endpoints/{created['id']}", headers=AUTH)
        assert resp.status_code == 404

    async def test_second_delete_returns_404(self, client: AsyncClient) -> None:
        created = await _create(client)
        await client.delete(f"/endpoints/{created['id']}", headers=AUTH)

        resp = await client.delete(f"/endpoints/{created['id']}", headers=AUTH)
        assert resp.status_code == 404

    async def test_unknown_id_returns_404(self, client: AsyncClient) -> None:
        resp = await client.delete(f"/endpoints/{uuid.uuid4()}", headers=AUTH)
        assert resp.status_code == 404

    async def test_missing_auth_returns_422(self, client: AsyncClient) -> None:
        created = await _create(client)
        resp = await client.delete(f"/endpoints/{created['id']}")
        assert resp.status_code == 422
