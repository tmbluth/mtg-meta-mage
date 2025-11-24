"""Unit tests for meta analytics service."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.app.api.services.meta_service import MetaService


class TestArchetypeRankings:
    """Tests for archetype ranking calculations."""

    def test_calculate_meta_share(self):
        """Test meta share calculation from decklist counts."""
        # Create sample data
        df = pl.DataFrame({
            "archetype_group_id": [1, 1, 1, 2, 2, 3],
            "main_title": ["amulet_titan", "amulet_titan", "amulet_titan", "burn", "burn", "elves"],
            "color_identity": ["gruul", "gruul", "gruul", "red", "red", "green"],
            "strategy": ["ramp", "ramp", "ramp", "aggro", "aggro", "aggro"],
        })

        service = MetaService()
        result = service._calculate_meta_share(df)

        # Total decklists: 6
        # amulet_titan: 3/6 = 50.0%
        # burn: 2/6 = 33.33%
        # elves: 1/6 = 16.67%
        assert len(result) == 3
        assert result["main_title"].to_list() == ["amulet_titan", "burn", "elves"]
        assert result["meta_share"][0] == pytest.approx(50.0, rel=0.01)
        assert result["meta_share"][1] == pytest.approx(33.33, rel=0.01)
        assert result["meta_share"][2] == pytest.approx(16.67, rel=0.01)

    def test_calculate_meta_share_empty_data(self):
        """Test meta share calculation with empty data returns empty DataFrame."""
        df = pl.DataFrame({
            "archetype_group_id": [],
            "main_title": [],
            "color_identity": [],
            "strategy": [],
        })

        service = MetaService()
        result = service._calculate_meta_share(df)

        assert len(result) == 0

    def test_calculate_win_rate(self):
        """Test win rate calculation from match results."""
        # Sample match data: archetype_group_id, is_win
        df = pl.DataFrame({
            "archetype_group_id": [1, 1, 1, 1, 2, 2, 2],
            "main_title": ["amulet_titan", "amulet_titan", "amulet_titan", "amulet_titan", "burn", "burn", "burn"],
            "is_win": [True, True, False, False, True, False, False],
        })

        service = MetaService()
        result = service._calculate_win_rate(df)

        # amulet_titan: 2 wins / 4 matches = 50.0%
        # burn: 1 win / 3 matches = 33.33%
        assert len(result) == 2
        amulet = result.filter(pl.col("main_title") == "amulet_titan")
        burn = result.filter(pl.col("main_title") == "burn")
        
        assert amulet["win_rate"][0] == pytest.approx(50.0, rel=0.01)
        assert amulet["match_count"][0] == 4
        assert burn["win_rate"][0] == pytest.approx(33.33, rel=0.01)
        assert burn["match_count"][0] == 3

    def test_calculate_win_rate_insufficient_data(self):
        """Test win rate returns None for archetypes with < 5 matches."""
        df = pl.DataFrame({
            "archetype_group_id": [1, 1, 1],
            "main_title": ["amulet_titan", "amulet_titan", "amulet_titan"],
            "is_win": [True, True, False],
        })

        service = MetaService()
        result = service._calculate_win_rate(df, min_matches=5)

        # Only 3 matches, should return None for win_rate
        assert len(result) == 1
        assert result["win_rate"][0] is None
        assert result["match_count"][0] == 3

    def test_filter_by_color_identity(self):
        """Test filtering archetypes by color identity."""
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["amulet_titan", "burn", "elves"],
            "color_identity": ["gruul", "red", "green"],
            "strategy": ["ramp", "aggro", "aggro"],
            "meta_share": [50.0, 30.0, 20.0],
        })

        service = MetaService()
        result = service._filter_by_color_identity(df, "red")

        assert len(result) == 1
        assert result["main_title"][0] == "burn"

    def test_filter_by_strategy(self):
        """Test filtering archetypes by strategy."""
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["amulet_titan", "burn", "elves"],
            "color_identity": ["gruul", "red", "green"],
            "strategy": ["ramp", "aggro", "aggro"],
            "meta_share": [50.0, 30.0, 20.0],
        })

        service = MetaService()
        result = service._filter_by_strategy(df, "aggro")

        assert len(result) == 2
        assert result["main_title"].to_list() == ["burn", "elves"]

    def test_group_by_color_identity(self):
        """Test grouping archetypes by color identity."""
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["burn", "rdw", "elves"],
            "color_identity": ["red", "red", "green"],
            "strategy": ["aggro", "aggro", "aggro"],
            "meta_share": [30.0, 20.0, 50.0],
            "sample_size": [10, 5, 15],
        })

        service = MetaService()
        result = service._group_by_color_identity(df)

        # Should have 2 groups: red (30+20=50%) and green (50%)
        assert len(result) == 2
        red_group = result.filter(pl.col("color_identity") == "red")
        green_group = result.filter(pl.col("color_identity") == "green")
        
        assert red_group["meta_share"][0] == pytest.approx(50.0, rel=0.01)
        assert red_group["sample_size"][0] == 15
        assert green_group["meta_share"][0] == pytest.approx(50.0, rel=0.01)
        assert green_group["sample_size"][0] == 15

    def test_group_by_strategy(self):
        """Test grouping archetypes by strategy."""
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["burn", "elves", "amulet_titan"],
            "color_identity": ["red", "green", "gruul"],
            "strategy": ["aggro", "aggro", "ramp"],
            "meta_share": [30.0, 20.0, 50.0],
            "sample_size": [10, 5, 15],
        })

        service = MetaService()
        result = service._group_by_strategy(df)

        # Should have 2 groups: aggro (30+20=50%) and ramp (50%)
        assert len(result) == 2
        aggro_group = result.filter(pl.col("strategy") == "aggro")
        ramp_group = result.filter(pl.col("strategy") == "ramp")
        
        assert aggro_group["meta_share"][0] == pytest.approx(50.0, rel=0.01)
        assert aggro_group["sample_size"][0] == 15
        assert ramp_group["meta_share"][0] == pytest.approx(50.0, rel=0.01)
        assert ramp_group["sample_size"][0] == 15

    def test_time_window_calculation(self):
        """Test time window date calculations."""
        service = MetaService()
        
        # Mock datetime to have a fixed reference point
        fixed_now = datetime(2025, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        
        with patch("src.app.api.services.meta_service.datetime") as mock_datetime:
            mock_datetime.now.return_value = fixed_now
            
            current_start, previous_start, previous_end = service._calculate_time_windows(
                current_days=14,
                previous_start_days=56,
                previous_end_days=14
            )
            
            # Current: last 14 days from fixed_now
            expected_current = fixed_now - timedelta(days=14)
            # Previous: 14-56 days ago
            expected_previous_end = fixed_now - timedelta(days=14)
            expected_previous_start = fixed_now - timedelta(days=56)
            
            assert current_start == expected_current
            assert previous_start == expected_previous_start
            assert previous_end == expected_previous_end


class TestMatchupMatrix:
    """Tests for matchup matrix calculations."""

    def test_calculate_matchup_matrix(self):
        """Test matchup matrix calculation from match results."""
        # Sample match data with player archetypes
        df = pl.DataFrame({
            "player_archetype": ["amulet_titan", "amulet_titan", "burn", "burn"],
            "opponent_archetype": ["burn", "burn", "amulet_titan", "amulet_titan"],
            "is_win": [True, False, True, False],
        })

        service = MetaService()
        result = service._calculate_matchup_matrix(df)

        # amulet_titan vs burn: 1 win / 2 matches = 50%
        # burn vs amulet_titan: 1 win / 2 matches = 50%
        assert "amulet_titan" in result
        assert "burn" in result
        assert "burn" in result["amulet_titan"]
        assert "amulet_titan" in result["burn"]
        
        assert result["amulet_titan"]["burn"]["win_rate"] == pytest.approx(50.0, rel=0.01)
        assert result["amulet_titan"]["burn"]["match_count"] == 2
        assert result["burn"]["amulet_titan"]["win_rate"] == pytest.approx(50.0, rel=0.01)
        assert result["burn"]["amulet_titan"]["match_count"] == 2

    def test_matchup_matrix_insufficient_data(self):
        """Test matchup matrix returns None for matchups with < 5 matches."""
        df = pl.DataFrame({
            "player_archetype": ["amulet_titan", "amulet_titan"],
            "opponent_archetype": ["burn", "burn"],
            "is_win": [True, False],
        })

        service = MetaService()
        result = service._calculate_matchup_matrix(df, min_matches=5)

        # Only 2 matches, should return None for win_rate
        assert result["amulet_titan"]["burn"]["win_rate"] is None
        assert result["amulet_titan"]["burn"]["match_count"] == 2

    def test_matchup_matrix_empty_data(self):
        """Test matchup matrix with empty data returns empty dict."""
        df = pl.DataFrame({
            "player_archetype": [],
            "opponent_archetype": [],
            "is_win": [],
        })

        service = MetaService()
        result = service._calculate_matchup_matrix(df)

        assert result == {}


class TestDatabaseQueries:
    """Tests for database query methods."""

    @patch("src.app.api.services.meta_service.DatabaseConnection")
    def test_fetch_archetype_data_for_format(self, mock_db):
        """Test fetching archetype data for a specific format."""
        # Mock cursor and query results
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "Modern", "amulet_titan", "gruul", "ramp", "2025-11-20"),
            (1, "Modern", "amulet_titan", "gruul", "ramp", "2025-11-19"),
            (2, "Modern", "burn", "red", "aggro", "2025-11-21"),
        ]
        mock_cursor.description = [
            ("archetype_group_id",), ("format",), ("main_title",),
            ("color_identity",), ("strategy",), ("tournament_date",)
        ]
        
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        service = MetaService()
        start_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
        end_date = datetime(2025, 11, 24, tzinfo=timezone.utc)
        
        result = service._fetch_archetype_data("Modern", start_date, end_date)

        assert len(result) == 3
        assert "main_title" in result.columns
        assert "strategy" in result.columns
        assert result["main_title"].to_list() == ["amulet_titan", "amulet_titan", "burn"]

    @patch("src.app.api.services.meta_service.DatabaseConnection")
    def test_fetch_match_data_for_format(self, mock_db):
        """Test fetching match data for a specific format."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "amulet_titan", 2, "burn", 1, "2025-11-20"),
            (2, "burn", 1, "amulet_titan", 2, "2025-11-21"),
        ]
        mock_cursor.description = [
            ("player_archetype_id",), ("player_archetype",),
            ("opponent_archetype_id",), ("opponent_archetype",),
            ("winner_id",), ("tournament_date",)
        ]
        
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        service = MetaService()
        start_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
        end_date = datetime(2025, 11, 24, tzinfo=timezone.utc)
        
        result = service._fetch_match_data("Modern", start_date, end_date)

        assert len(result) == 2
        assert "player_archetype" in result.columns
        assert "opponent_archetype" in result.columns
        assert result["player_archetype"].to_list() == ["amulet_titan", "burn"]

