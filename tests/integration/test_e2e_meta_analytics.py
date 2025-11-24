"""End-to-end integration tests for meta analytics API."""

import logging
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.app.api.main import app
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@pytest.fixture
def sample_meta_data(test_database):
    """
    Create sample tournament, archetype, and match data for meta analytics tests.
    
    Creates:
    - 2 tournaments (1 recent, 1 older) in Modern format
    - 3 archetype groups (amulet_titan, burn, elves)
    - 10 decklists across both tournaments
    - 15 matches with winners
    """
    with DatabaseConnection.transaction() as conn:
        cur = conn.cursor()
        
        # Create tournaments
        now = datetime.now(timezone.utc)
        recent_date = now - timedelta(days=7)  # Within current period (last 14 days)
        older_date = now - timedelta(days=30)  # Within previous period (14-56 days ago)
        
        cur.execute("""
            INSERT INTO tournaments (tournament_id, tournament_name, format, start_date, swiss_num, top_cut)
            VALUES 
                ('t1', 'Modern Tournament 1', 'Modern', %s, 5, 8),
                ('t2', 'Modern Tournament 2', 'Modern', %s, 5, 8)
        """, (recent_date, older_date))
        
        # Create archetype groups
        cur.execute("""
            INSERT INTO archetype_groups (archetype_group_id, format, main_title, color_identity, strategy)
            VALUES 
                (1, 'Modern', 'amulet_titan', 'gruul', 'ramp'),
                (2, 'Modern', 'burn', 'red', 'aggro'),
                (3, 'Modern', 'elves', 'green', 'aggro')
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
        
        # Create decklists for tournament 1
        # 3 amulet_titan, 1 burn, 1 elves
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t1_p1', 't1', 1),
                ('t1_p2', 't1', 1),
                ('t1_p3', 't1', 1),
                ('t1_p4', 't1', 2),
                ('t1_p5', 't1', 3)
        """)
        
        # Create decklists for tournament 2
        # 2 amulet_titan, 2 burn, 1 elves
        cur.execute("""
            INSERT INTO decklists (player_id, tournament_id, archetype_group_id)
            VALUES 
                ('t2_p1', 't2', 1),
                ('t2_p2', 't2', 1),
                ('t2_p3', 't2', 2),
                ('t2_p4', 't2', 2),
                ('t2_p5', 't2', 3)
        """)
        
        # Create match rounds
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
        
        # Create matches for tournament 1
        # amulet_titan (p1) beats burn (p4)
        # amulet_titan (p2) beats elves (p5)
        # amulet_titan (p3) loses to burn (p4)
        # amulet_titan (p1) beats amulet_titan (p2)
        # burn (p4) beats elves (p5)
        cur.execute("""
            INSERT INTO matches (round_number, tournament_id, match_num, player1_id, player2_id, winner_id, status)
            VALUES 
                (1, 't1', 1, 't1_p1', 't1_p4', 't1_p1', 'completed'),
                (1, 't1', 2, 't1_p2', 't1_p5', 't1_p2', 'completed'),
                (2, 't1', 1, 't1_p3', 't1_p4', 't1_p4', 'completed'),
                (2, 't1', 2, 't1_p1', 't1_p2', 't1_p1', 'completed'),
                (3, 't1', 1, 't1_p4', 't1_p5', 't1_p4', 'completed')
        """)
        
        # Create matches for tournament 2
        # amulet_titan (p1) beats burn (p3)
        # amulet_titan (p2) loses to burn (p4)
        # burn (p3) beats elves (p5)
        # amulet_titan (p1) beats amulet_titan (p2)
        # burn (p3) beats burn (p4)
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
        response = api_client.get("/api/v1/meta/archetypes?format=Modern")
        
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
        # Query with current period as last 7 days
        response = api_client.get("/api/v1/meta/archetypes?format=Modern&current_days=7")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have data from tournament 1 (7 days ago)
        assert len(data["data"]) > 0
        
        # Current period should have 5 decklists (from t1)
        total_current_sample = sum(a.get("sample_size_current", 0) for a in data["data"])
        assert total_current_sample == 5

    def test_get_archetype_rankings_filter_by_color_identity(self, sample_meta_data, api_client):
        """Test filtering by color identity."""
        response = api_client.get("/api/v1/meta/archetypes?format=Modern&color_identity=red")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only return burn archetype (red)
        assert all(a["color_identity"] == "red" for a in data["data"])
        assert any(a["main_title"] == "burn" for a in data["data"])

    def test_get_archetype_rankings_filter_by_strategy(self, sample_meta_data, api_client):
        """Test filtering by strategy."""
        response = api_client.get("/api/v1/meta/archetypes?format=Modern&strategy=aggro")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only return aggro archetypes (burn, elves)
        assert all(a["strategy"] == "aggro" for a in data["data"])
        assert len(data["data"]) == 2

    def test_get_archetype_rankings_group_by_color_identity(self, sample_meta_data, api_client):
        """Test grouping by color identity."""
        response = api_client.get("/api/v1/meta/archetypes?format=Modern&group_by=color_identity")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have 3 groups: gruul, red, green
        color_identities = [a["color_identity"] for a in data["data"]]
        assert "gruul" in color_identities
        assert "red" in color_identities
        assert "green" in color_identities

    def test_get_archetype_rankings_group_by_strategy(self, sample_meta_data, api_client):
        """Test grouping by strategy."""
        response = api_client.get("/api/v1/meta/archetypes?format=Modern&group_by=strategy")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have 2 groups: ramp, aggro
        strategies = [a["strategy"] for a in data["data"]]
        assert "ramp" in strategies
        assert "aggro" in strategies

    def test_get_archetype_rankings_invalid_format(self, sample_meta_data, api_client):
        """Test that invalid format returns 404."""
        response = api_client.get("/api/v1/meta/archetypes?format=InvalidFormat")
        
        assert response.status_code == 404
        data = response.json()
        assert "error" in data["detail"]

    def test_get_archetype_rankings_overlapping_time_windows(self, sample_meta_data, api_client):
        """Test that overlapping time windows return 400."""
        response = api_client.get(
            "/api/v1/meta/archetypes?format=Modern&current_days=14&previous_end_days=15"
        )
        
        assert response.status_code == 400
        data = response.json()
        assert "overlap" in data["detail"]["message"].lower()


@pytest.mark.integration
class TestMatchupMatrixIntegration:
    """Integration tests for matchup matrix endpoint."""

    def test_get_matchup_matrix_with_real_data(self, sample_meta_data, api_client):
        """Test matchup matrix with real database data."""
        response = api_client.get("/api/v1/meta/matchups?format=Modern")
        
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
        """Test that specific matchup data is calculated correctly."""
        response = api_client.get("/api/v1/meta/matchups?format=Modern&days=40")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have amulet_titan vs burn matchup
        if "amulet_titan" in data["matrix"] and "burn" in data["matrix"]["amulet_titan"]:
            matchup = data["matrix"]["amulet_titan"]["burn"]
            assert matchup["match_count"] > 0
            # Win rate should be between 0 and 100 if there's enough data
            if matchup["win_rate"] is not None:
                assert 0 <= matchup["win_rate"] <= 100

    def test_get_matchup_matrix_custom_time_window(self, sample_meta_data, api_client):
        """Test matchup matrix with custom time window."""
        # Only include last 7 days (should only have t1 matches)
        response = api_client.get("/api/v1/meta/matchups?format=Modern&days=7")
        
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

