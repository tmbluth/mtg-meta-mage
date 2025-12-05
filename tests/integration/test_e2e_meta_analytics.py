"""End-to-end integration tests for meta analytics API."""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.app.api.main import app
from src.etl.database.connection import DatabaseConnection
from src.app.mcp.tools.meta_research_tools import (
    get_format_meta_rankings,
    get_format_matchup_stats
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def mock_mcp_client():
    """
    Mock MCP client to call tool functions directly instead of over HTTP.
    
    This allows integration tests to test the full stack (database → tools → API)
    without requiring an HTTP MCP server to be running.
    """
    async def call_mcp_tool_direct(tool_name: str, arguments: dict) -> dict:
        """Call MCP tool functions directly, bypassing HTTP."""
        # FastMCP tools are FunctionTool objects - access underlying function via .fn
        if tool_name == "get_format_meta_rankings":
            return get_format_meta_rankings.fn(**arguments)
        elif tool_name == "get_format_matchup_stats":
            return get_format_matchup_stats.fn(**arguments)
        else:
            raise KeyError(f"Unknown tool: {tool_name}")
    
    with patch("src.app.api.routes.meta_routes.call_mcp_tool", new_callable=AsyncMock) as mock:
        mock.side_effect = call_mcp_tool_direct
        yield mock


@pytest.fixture
def sample_meta_data(test_database):
    """
    Create sample tournament, archetype, and match data for meta analytics tests.

    Creates:
    - 2 tournaments (1 recent, 1 older) in Standard format
    - 3 archetype groups from Standard metagame (esper_midrange, domain_ramp, boros_convoke)
    - 10 decklists across both tournaments
    - 15 matches with winners
    """
    with DatabaseConnection.transaction() as conn:
        cur = conn.cursor()

        # Use Standard format and current top archetypes
        now = datetime.now(timezone.utc)
        recent_date = now - timedelta(days=7)  # Within current period (last 14 days)
        older_date = now - timedelta(days=30)  # Within previous period (14-56 days ago)

        # Create tournaments in Standard format
        cur.execute("""
            INSERT INTO tournaments (tournament_id, tournament_name, format, start_date, swiss_num, top_cut)
            VALUES 
                ('t1', 'Standard Showdown 1', 'Standard', %s, 5, 8),
                ('t2', 'Standard Showdown 2', 'Standard', %s, 5, 8)
        """, (recent_date, older_date))

        # Create Standard archetype groups (realistic, recent names)
        cur.execute("""
            INSERT INTO archetype_groups (archetype_group_id, format, main_title, color_identity, strategy)
            VALUES 
                (1, 'Standard', 'esper_midrange', 'esper', 'midrange'),
                (2, 'Standard', 'domain_ramp', 'five_color', 'ramp'),
                (3, 'Standard', 'boros_convoke', 'boros', 'aggro')
        """)

        # Create players for tournament 1 (recent)
        players_t1 = []
        for i in range(1, 6):
            player_id = f't1_p{i}'
            players_t1.append(player_id)
            cur.execute("""
                INSERT INTO players (player_id, tournament_id, name, wins, losses, standing)
                VALUES (%s, 't1', %s, 3, 2, %s)
            """, (player_id, f'Player {i}', i))

        # Create players for tournament 2 (older)
        players_t2 = []
        for i in range(1, 6):
            player_id = f't2_p{i}'
            players_t2.append(player_id)
            cur.execute("""
                INSERT INTO players (player_id, tournament_id, name, wins, losses, standing)
                VALUES (%s, 't2', %s, 3, 2, %s)
            """, (player_id, f'Player {i}', i))

        # Create decklists for tournament 1 (Standard Showdown 1)
        # 3 esper_midrange, 1 domain_ramp, 1 boros_convoke
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t1_p1', 't1', 1),
                ('t1_p2', 't1', 1),
                ('t1_p3', 't1', 1),
                ('t1_p4', 't1', 2),
                ('t1_p5', 't1', 3)
        """)

        # Create decklists for tournament 2 (Standard Showdown 2)
        # 2 esper_midrange, 2 domain_ramp, 1 boros_convoke
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t2_p1', 't2', 1),
                ('t2_p2', 't2', 1),
                ('t2_p3', 't2', 2),
                ('t2_p4', 't2', 2),
                ('t2_p5', 't2', 3)
        """)

        # Create match rounds for both tournaments
        cur.execute("""
            INSERT INTO match_rounds (round_number, tournament_id, round_description)
            VALUES 
                (1, 't1', 'Round 1'),
                (2, 't1', 'Round 2'),
                (3, 't1', 'Round 3'),
                (1, 't2', 'Round 1'),
                (2, 't2', 'Round 2'),
                (3, 't2', 'Round 3')
        """)

        # Create matches for tournament 1 (Standard)
        # esper_midrange (p1) beats boros_convoke (p5)
        # esper_midrange (p2) beats domain_ramp (p4)
        # esper_midrange (p3) loses to boros_convoke (p5)
        # esper_midrange (p1) beats esper_midrange (p2)
        # boros_convoke (p5) beats domain_ramp (p4)
        cur.execute("""
            INSERT INTO matches (round_number, tournament_id, match_num, player1_id, player2_id, winner_id, status)
            VALUES 
                (1, 't1', 1, 't1_p1', 't1_p5', 't1_p1', 'completed'),
                (1, 't1', 2, 't1_p2', 't1_p4', 't1_p2', 'completed'),
                (2, 't1', 1, 't1_p3', 't1_p5', 't1_p5', 'completed'),
                (2, 't1', 2, 't1_p1', 't1_p2', 't1_p1', 'completed'),
                (3, 't1', 1, 't1_p5', 't1_p4', 't1_p5', 'completed')
        """)

        # Create matches for tournament 2 (Standard)
        # esper_midrange (p1) beats domain_ramp (p3)
        # esper_midrange (p2) loses to domain_ramp (p4)
        # domain_ramp (p3) beats boros_convoke (p5)
        # esper_midrange (p1) beats esper_midrange (p2)
        # domain_ramp (p3) beats domain_ramp (p4)
        cur.execute("""
            INSERT INTO matches (round_number, tournament_id, match_num, player1_id, player2_id, winner_id, status)
            VALUES 
                (1, 't2', 1, 't2_p1', 't2_p3', 't2_p1', 'completed'),
                (1, 't2', 2, 't2_p2', 't2_p4', 't2_p4', 'completed'),
                (2, 't2', 1, 't2_p3', 't2_p5', 't2_p3', 'completed'),
                (2, 't2', 2, 't2_p1', 't2_p2', 't2_p1', 'completed'),
                (3, 't2', 1, 't2_p3', 't2_p4', 't2_p3', 'completed')
        """)

        cur.close()


@pytest.fixture
def api_client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.mark.integration
class TestArchetypeRankingsIntegration:
    """Integration tests for archetype rankings endpoint."""

    def test_get_archetype_rankings_with_real_data(self, sample_meta_data, api_client):
        """Test archetype rankings with real database data."""
        response = api_client.get("/api/v1/meta/archetypes?format=Standard")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) > 0
        
        # Check that all required fields are present
        first_archetype = data["data"][0]
        assert "main_title" in first_archetype
        assert "color_identity" in first_archetype
        assert "strategy" in first_archetype
        assert "meta_share_current" in first_archetype
        assert "sample_size_current" in first_archetype
        
        # Verify meta share adds up to roughly 100%
        total_meta_share = sum(a["meta_share_current"] for a in data["data"])
        assert 99.0 <= total_meta_share <= 101.0  # Allow small rounding errors

    def test_get_archetype_rankings_with_time_window_filtering(self, sample_meta_data, api_client):
        """Test time window filtering works correctly."""
        # Query with current period as last 10 days and previous period as 14 days before that
        response = api_client.get("/api/v1/meta/archetypes?format=Standard&current_days=10&previous_days=14")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have data from tournament 1 (7 days ago)
        assert len(data["data"]) > 0
        
        # Current period should have 5 decklists (from t1)
        total_current_sample = sum(a.get("sample_size_current", 0) for a in data["data"])
        assert total_current_sample == 5

    def test_get_archetype_rankings_filter_by_color_identity(self, sample_meta_data, api_client):
        """Test filtering by color identity."""
        response = api_client.get("/api/v1/meta/archetypes?format=Standard&color_identity=esper")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only return esper_midrange archetype (esper)
        assert all(a["color_identity"] == "esper" for a in data["data"])
        assert any(a["main_title"] == "esper_midrange" for a in data["data"])

    def test_get_archetype_rankings_filter_by_strategy(self, sample_meta_data, api_client):
        """Test filtering by strategy."""
        response = api_client.get("/api/v1/meta/archetypes?format=Standard&strategy=aggro")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only return aggro archetypes (boros_convoke)
        assert all(a["strategy"] == "aggro" for a in data["data"])
        assert len(data["data"]) == 1

    def test_get_archetype_rankings_group_by_color_identity(self, sample_meta_data, api_client):
        """Test grouping by color identity."""
        response = api_client.get("/api/v1/meta/archetypes?format=Standard&group_by=color_identity")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have 3 groups: esper, five_color, boros
        color_identities = [a["color_identity"] for a in data["data"]]
        assert "esper" in color_identities
        assert "five_color" in color_identities
        assert "boros" in color_identities

    def test_get_archetype_rankings_group_by_strategy(self, sample_meta_data, api_client):
        """Test grouping by strategy."""
        response = api_client.get("/api/v1/meta/archetypes?format=Standard&group_by=strategy")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have 3 groups: midrange, ramp, aggro
        strategies = [a["strategy"] for a in data["data"]]
        assert "midrange" in strategies
        assert "ramp" in strategies
        assert "aggro" in strategies

    def test_get_archetype_rankings_invalid_format(self, sample_meta_data, api_client):
        """Test that invalid format returns 404."""
        response = api_client.get("/api/v1/meta/archetypes?format=InvalidFormat")
        
        assert response.status_code == 404
        data = response.json()
        assert "error" in data["detail"]

    def test_get_archetype_rankings_with_longer_previous_period(self, sample_meta_data, api_client):
        """Test with a longer previous period for more historical data."""
        response = api_client.get(
            "/api/v1/meta/archetypes?format=Standard&current_days=14&previous_days=28"
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) > 0


@pytest.mark.integration
class TestMatchupMatrixIntegration:
    """Integration tests for matchup matrix endpoint."""

    def test_get_matchup_matrix_with_real_data(self, sample_meta_data, api_client):
        """Test matchup matrix with real database data."""
        response = api_client.get("/api/v1/meta/matchups?format=Standard")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "matrix" in data
        assert "archetypes" in data
        assert "metadata" in data
        assert len(data["archetypes"]) > 0
        
        # Check matrix structure
        for archetype in data["archetypes"]:
            if archetype in data["matrix"]:
                for opponent, matchup in data["matrix"][archetype].items():
                    assert "win_rate" in matchup
                    assert "match_count" in matchup
                    # Win rate can be None if insufficient data
                    if matchup["win_rate"] is not None:
                        assert 0 <= matchup["win_rate"] <= 100

    def test_get_matchup_matrix_specific_matchup(self, sample_meta_data, api_client):
        """Test that specific Standard matchup data is calculated correctly."""
        response = api_client.get("/api/v1/meta/matchups?format=Standard&days=40")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have esper_midrange vs boros_convoke matchup
        if "esper_midrange" in data["matrix"] and "boros_convoke" in data["matrix"]["esper_midrange"]:
            matchup = data["matrix"]["esper_midrange"]["boros_convoke"]
            assert matchup["match_count"] > 0
            # Win rate should be between 0 and 100 if there's enough data
            if matchup["win_rate"] is not None:
                assert 0 <= matchup["win_rate"] <= 100

    def test_get_matchup_matrix_custom_time_window(self, sample_meta_data, api_client):
        """Test matchup matrix with custom time window."""
        # Include last 10 days (should capture t1 matches at 7 days ago)
        response = api_client.get("/api/v1/meta/matchups?format=Standard&days=10")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have matchup data from recent tournament
        assert len(data["matrix"]) > 0

    def test_get_matchup_matrix_invalid_format(self, sample_meta_data, api_client):
        """Test that invalid format returns 404."""
        response = api_client.get("/api/v1/meta/matchups?format=InvalidFormat")
        
        assert response.status_code == 404
        data = response.json()
        assert "error" in data["detail"]

    def test_get_matchup_matrix_no_data(self, api_client):
        """Test matchup matrix with no available data."""
        # Query a format with no data
        response = api_client.get("/api/v1/meta/matchups?format=Vintage")
        
        assert response.status_code == 404


@pytest.mark.integration
class TestHealthEndpoint:
    """Integration tests for health check endpoint."""

    def test_health_check(self, api_client):
        """Test health check endpoint."""
        response = api_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

