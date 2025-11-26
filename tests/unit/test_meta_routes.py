"""Unit tests for meta analytics API routes."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.app.api.main import app

client = TestClient(app)


class TestArchetypeEndpoint:
    """Tests for GET /api/v1/meta/archetypes endpoint."""

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_success(self, mock_service_class):
        """Test successful archetype rankings request."""
        # Mock service response
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.return_value = {
            "data": [
                {
                    "main_title": "amulet_titan",
                    "color_identity": "gruul",
                    "strategy": "ramp",
                    "meta_share_current": 15.5,
                    "meta_share_previous": 12.0,
                    "win_rate_current": 52.3,
                    "win_rate_previous": 50.1,
                    "sample_size_current": 25,
                    "sample_size_previous": 20,
                    "match_count_current": 100,
                    "match_count_previous": 80,
                }
            ],
            "metadata": {
                "format": "Modern",
                "current_period": {"days": 14},
                "previous_period": {"days": 14},
            },
        }
        mock_service_class.return_value = mock_service

        # Make request
        response = client.get("/api/v1/meta/archetypes?format=Modern")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["main_title"] == "amulet_titan"

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_with_filters(self, mock_service_class):
        """Test archetype rankings with color and strategy filters."""
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.return_value = {
            "data": [
                {
                    "main_title": "burn",
                    "color_identity": "red",
                    "strategy": "aggro",
                    "meta_share_current": 10.0,
                    "meta_share_previous": 9.5,
                    "win_rate_current": 48.5,
                    "win_rate_previous": 47.0,
                    "sample_size_current": 15,
                    "sample_size_previous": 12,
                    "match_count_current": 60,
                    "match_count_previous": 50,
                }
            ],
            "metadata": {"format": "Modern"},
        }
        mock_service_class.return_value = mock_service

        response = client.get(
            "/api/v1/meta/archetypes?format=Modern&color_identity=red&strategy=aggro"
        )

        assert response.status_code == 200
        mock_service.get_archetype_rankings.assert_called_once()
        call_kwargs = mock_service.get_archetype_rankings.call_args[1]
        assert call_kwargs["color_identity"] == "red"
        assert call_kwargs["strategy"] == "aggro"

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_with_custom_time_windows(self, mock_service_class):
        """Test archetype rankings with custom time window parameters."""
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.return_value = {
            "data": [],
            "metadata": {"format": "Pioneer"},
        }
        mock_service_class.return_value = mock_service

        response = client.get(
            "/api/v1/meta/archetypes?format=Pioneer&current_days=7&previous_days=21"
        )

        assert response.status_code == 200
        call_kwargs = mock_service.get_archetype_rankings.call_args[1]
        assert call_kwargs["current_days"] == 7
        assert call_kwargs["previous_days"] == 21

    def test_get_archetype_rankings_missing_format(self):
        """Test that missing format parameter returns 422."""
        response = client.get("/api/v1/meta/archetypes")
        assert response.status_code == 422  # Validation error

    def test_get_archetype_rankings_invalid_strategy(self):
        """Test that invalid strategy value returns 422."""
        response = client.get("/api/v1/meta/archetypes?format=Modern&strategy=invalid")
        assert response.status_code == 422

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_with_different_period_lengths(self, mock_service_class):
        """Test with different current and previous period lengths."""
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.return_value = {
            "data": [],
            "metadata": {"format": "Modern"},
        }
        mock_service_class.return_value = mock_service

        # Current: 14 days, Previous: 30 days
        response = client.get(
            "/api/v1/meta/archetypes?format=Modern&current_days=14&previous_days=30"
        )

        assert response.status_code == 200
        call_kwargs = mock_service.get_archetype_rankings.call_args[1]
        assert call_kwargs["current_days"] == 14
        assert call_kwargs["previous_days"] == 30

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_with_grouping(self, mock_service_class):
        """Test archetype rankings with group_by parameter."""
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.return_value = {
            "data": [
                {
                    "color_identity": "red",
                    "meta_share_current": 25.0,
                    "sample_size_current": 40,
                }
            ],
            "metadata": {"format": "Modern"},
        }
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/archetypes?format=Modern&group_by=color_identity")

        assert response.status_code == 200
        call_kwargs = mock_service.get_archetype_rankings.call_args[1]
        assert call_kwargs["group_by"] == "color_identity"

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_archetype_rankings_service_error(self, mock_service_class):
        """Test that service errors are handled properly."""
        mock_service = MagicMock()
        mock_service.get_archetype_rankings.side_effect = Exception("Database connection failed")
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/archetypes?format=Modern")

        assert response.status_code == 500
        assert "error" in response.json()


class TestMatchupEndpoint:
    """Tests for GET /api/v1/meta/matchups endpoint."""

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_matchup_matrix_success(self, mock_service_class):
        """Test successful matchup matrix request."""
        mock_service = MagicMock()
        mock_service.get_matchup_matrix.return_value = {
            "matrix": {
                "rhinos": {
                    "scam": {"win_rate": 60.0, "match_count": 18},
                    "yawgmoth": {"win_rate": 49.5, "match_count": 12},
                },
                "scam": {
                    "rhinos": {"win_rate": 40.0, "match_count": 18},
                    "yawgmoth": {"win_rate": 55.0, "match_count": 17},
                },
            },
            "archetypes": ["rhinos", "scam"],
            "metadata": {"format": "Modern", "days": 14},
        }
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/matchups?format=Modern")

        assert response.status_code == 200
        data = response.json()
        assert "matrix" in data
        assert "archetypes" in data
        assert "metadata" in data
        assert "rhinos" in data["matrix"]
        assert "scam" in data["matrix"]["rhinos"]

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_matchup_matrix_with_custom_days(self, mock_service_class):
        """Test matchup matrix with custom days parameter."""
        mock_service = MagicMock()
        mock_service.get_matchup_matrix.return_value = {
            "matrix": {},
            "archetypes": [],
            "metadata": {"format": "Pioneer", "days": 30},
        }
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/matchups?format=Pioneer&days=30")

        assert response.status_code == 200
        call_kwargs = mock_service.get_matchup_matrix.call_args[1]
        assert call_kwargs["days"] == 30

    def test_get_matchup_matrix_missing_format(self):
        """Test that missing format parameter returns 422."""
        response = client.get("/api/v1/meta/matchups")
        assert response.status_code == 422

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_matchup_matrix_no_data(self, mock_service_class):
        """Test matchup matrix with no available data."""
        mock_service = MagicMock()
        mock_service.get_matchup_matrix.return_value = {
            "matrix": {},
            "archetypes": [],
            "metadata": {"format": "Vintage", "days": 14},
        }
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/matchups?format=Vintage")

        assert response.status_code == 404
        assert "no data" in response.json()["message"].lower()

    @patch("src.app.api.routes.meta_routes.MetaService")
    def test_get_matchup_matrix_service_error(self, mock_service_class):
        """Test that service errors are handled properly."""
        mock_service = MagicMock()
        mock_service.get_matchup_matrix.side_effect = Exception("Database error")
        mock_service_class.return_value = mock_service

        response = client.get("/api/v1/meta/matchups?format=Modern")

        assert response.status_code == 500
        assert "error" in response.json()


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_check(self):
        """Test health check endpoint."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

