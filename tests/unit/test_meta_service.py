"""Unit tests for meta analytics service."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.app.api.services.meta_analysis import MetaService


class TestArchetypeRankings:
    """Tests for archetype ranking calculations."""

    def test_calculate_meta_share(self):
        """Test meta share calculation from decklist counts (Pioneer format)."""
        # Create sample data using Pioneer archetypes
        df = pl.DataFrame({
            "archetype_group_id": [1, 1, 1, 2, 2, 3],
            "main_title": ["rakdos_midrange", "rakdos_midrange", "rakdos_midrange", "phoenix", "phoenix", "lotus_field"],
            "color_identity": ["rakdos", "rakdos", "rakdos", "izzet", "izzet", "simic"],
            "strategy": ["midrange", "midrange", "midrange", "aggro", "aggro", "combo"],
        })

        service = MetaService()
        result = service._calculate_meta_share(df)

        # Total decklists: 6
        # rakdos_midrange: 3/6 = 50.0%
        # phoenix: 2/6 = 33.33%
        # lotus_field: 1/6 = 16.67%
        assert len(result) == 3
        assert result["main_title"].to_list() == ["rakdos_midrange", "phoenix", "lotus_field"]
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
        """Test win rate calculation from match results (Standard format)."""
        # Sample match data using Standard archetypes with proper database columns
        # Need at least 5 matches per archetype (default min_matches=5)
        df = pl.DataFrame({
            "player_archetype_id": [1, 1, 2, 2, 1, 2],
            "player_archetype": ["esper_midrange", "esper_midrange", "domain_ramp", "domain_ramp", "esper_midrange", "domain_ramp"],
            "opponent_archetype_id": [2, 2, 1, 1, 2, 1],
            "opponent_archetype": ["domain_ramp", "domain_ramp", "esper_midrange", "esper_midrange", "domain_ramp", "esper_midrange"],
            "player1_id": ["p1", "p2", "p3", "p4", "p5", "p6"],
            "player2_id": ["p8", "p9", "p10", "p11", "p12", "p13"],
            "winner_id": ["p1", "p9", "p3", "p4", "p12", "p13"],
        })

        service = MetaService()
        result = service._calculate_win_rate(df)

        # Method creates both perspectives (player1 and player2):
        # esper_midrange as player1: p1 wins, p2 loses, p5 loses = 1 win / 3 matches
        # esper_midrange as player2: p10 loses, p11 loses, p13 wins = 1 win / 3 matches
        # Total esper_midrange: 2 wins / 6 matches = 33.33%
        # domain_ramp as player1: p3 wins, p4 wins, p6 loses = 2 wins / 3 matches
        # domain_ramp as player2: p8 loses, p9 wins, p12 wins = 2 wins / 3 matches
        # Total domain_ramp: 4 wins / 6 matches = 66.67%
        assert len(result) == 2
        esper = result.filter(pl.col("main_title") == "esper_midrange")
        domain = result.filter(pl.col("main_title") == "domain_ramp")
        
        assert esper["win_rate"][0] == pytest.approx(33.33, rel=0.01)
        assert esper["match_count"][0] == 6
        assert domain["win_rate"][0] == pytest.approx(66.67, rel=0.01)
        assert domain["match_count"][0] == 6

    def test_calculate_win_rate_insufficient_data(self):
        """Test win rate returns None for archetypes with < 5 matches (Legacy format)."""
        # Use Legacy archetype with proper database columns
        df = pl.DataFrame({
            "player_archetype_id": [1, 1, 1],
            "player_archetype": ["death_and_taxes", "death_and_taxes", "death_and_taxes"],
            "opponent_archetype_id": [2, 2, 2],
            "opponent_archetype": ["delver", "delver", "delver"],
            "player1_id": ["p1", "p2", "p3"],
            "player2_id": ["p4", "p5", "p6"],
            "winner_id": ["p1", "p2", "p6"],
        })

        service = MetaService()
        result = service._calculate_win_rate(df, min_matches=5)

        # Only 3 matches, should return None for win_rate
        assert len(result) == 2  # Both archetypes appear
        dnt = result.filter(pl.col("main_title") == "death_and_taxes")
        assert dnt["win_rate"][0] is None
        assert dnt["match_count"][0] == 3

    def test_filter_by_color_identity(self):
        """Test filtering archetypes by color identity (Pauper format)."""
        # Use Pauper archetypes
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["affinity", "bogles", "caw_gate"],
            "color_identity": ["blue_red", "green_white", "azorius"],
            "strategy": ["aggro", "aggro", "control"],
            "meta_share": [50.0, 30.0, 20.0],
        })

        service = MetaService()
        result = service._filter_by_color_identity(df, "azorius")

        assert len(result) == 1
        assert result["main_title"][0] == "caw_gate"

    def test_filter_by_strategy(self):
        """Test filtering archetypes by strategy (Modern format)."""
        # Use Modern archetypes
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["rhinos", "yawgmoth", "living_end"],
            "color_identity": ["temur", "golgari", "jund"],
            "strategy": ["midrange", "combo", "combo"],
            "meta_share": [50.0, 30.0, 20.0],
        })

        service = MetaService()
        result = service._filter_by_strategy(df, "combo")

        assert len(result) == 2
        assert result["main_title"].to_list() == ["yawgmoth", "living_end"]

    def test_group_by_color_identity(self):
        """Test grouping archetypes by color identity (Vintage format)."""
        # Use Vintage archetypes with proper column names from _merge_period_data
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["doomsday", "tinker", "oath"],
            "color_identity": ["grixis", "blue", "blue"],
            "strategy": ["combo", "combo", "combo"],
            "meta_share_current": [30.0, 20.0, 50.0],
            "sample_size_current": [10, 5, 15],
            "meta_share_previous": [25.0, 18.0, 45.0],
            "sample_size_previous": [8, 4, 12],
            "win_rate_current": [52.0, 55.0, 50.0],
            "win_rate_previous": [51.0, 53.0, 49.0],
            "match_count_current": [100, 80, 120],
            "match_count_previous": [90, 75, 110],
        })

        service = MetaService()
        result = service._group_by_color_identity(df)

        # Should have 2 groups: blue (20+50=70%) and grixis (30%)
        assert len(result) == 2
        blue_group = result.filter(pl.col("color_identity") == "blue")
        grixis_group = result.filter(pl.col("color_identity") == "grixis")
        
        assert blue_group["meta_share_current"][0] == pytest.approx(70.0, rel=0.01)
        assert blue_group["sample_size_current"][0] == 20
        assert grixis_group["meta_share_current"][0] == pytest.approx(30.0, rel=0.01)
        assert grixis_group["sample_size_current"][0] == 10

    def test_group_by_strategy(self):
        """Test grouping archetypes by strategy (Standard format)."""
        # Use different Standard archetypes than before with proper column names
        df = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["boros_convoke", "mono_red_aggro", "azorius_control"],
            "color_identity": ["boros", "red", "azorius"],
            "strategy": ["aggro", "aggro", "control"],
            "meta_share_current": [30.0, 20.0, 50.0],
            "sample_size_current": [10, 5, 15],
            "meta_share_previous": [28.0, 19.0, 48.0],
            "sample_size_previous": [9, 4, 14],
            "win_rate_current": [48.0, 47.0, 53.0],
            "win_rate_previous": [47.0, 46.0, 52.0],
            "match_count_current": [95, 85, 130],
            "match_count_previous": [88, 80, 125],
        })

        service = MetaService()
        result = service._group_by_strategy(df)

        # Should have 2 groups: aggro (30+20=50%) and control (50%)
        assert len(result) == 2
        aggro_group = result.filter(pl.col("strategy") == "aggro")
        control_group = result.filter(pl.col("strategy") == "control")
        
        assert aggro_group["meta_share_current"][0] == pytest.approx(50.0, rel=0.01)
        assert aggro_group["sample_size_current"][0] == 15
        assert control_group["meta_share_current"][0] == pytest.approx(50.0, rel=0.01)
        assert control_group["sample_size_current"][0] == 15

    def test_time_window_calculation(self):
        """Test time window date calculations."""
        service = MetaService()
        
        # Mock datetime to have a fixed reference point
        fixed_now = datetime(2025, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        
        with patch("src.app.api.services.meta_analysis.datetime") as mock_datetime:
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
        """Test matchup matrix calculation from match results (Legacy format)."""
        # Sample match data using Legacy archetypes with proper database columns
        # Need at least 5 matches per matchup for valid win_rate (default min_matches=5)
        df = pl.DataFrame({
            "player_archetype_id": [1, 1, 2, 2, 1, 2],
            "player_archetype": ["delver", "delver", "reanimator", "reanimator", "delver", "reanimator"],
            "opponent_archetype_id": [2, 2, 1, 1, 2, 1],
            "opponent_archetype": ["reanimator", "reanimator", "delver", "delver", "reanimator", "delver"],
            "player1_id": ["p1", "p2", "p3", "p4", "p5", "p6"],
            "player2_id": ["p7", "p8", "p9", "p10", "p11", "p12"],
            "winner_id": ["p1", "p8", "p3", "p10", "p5", "p12"],
        })

        service = MetaService()
        result = service._calculate_matchup_matrix(df)

        # Method creates both player1 and player2 perspectives:
        # delver as player1 vs reanimator: p1 wins, p2 loses, p5 wins = 2 wins / 3 matches
        # delver as player2 vs reanimator: p9 loses (p3 wins), p10 wins, p12 wins = 2 wins / 3 matches
        # Total delver vs reanimator: 4 wins / 6 matches = 66.67%
        # reanimator vs delver: 2 wins / 6 matches = 33.33%
        assert "delver" in result
        assert "reanimator" in result
        assert "reanimator" in result["delver"]
        assert "delver" in result["reanimator"]
        
        assert result["delver"]["reanimator"]["win_rate"] == pytest.approx(66.67, rel=0.01)
        assert result["delver"]["reanimator"]["match_count"] == 6
        assert result["reanimator"]["delver"]["win_rate"] == pytest.approx(33.33, rel=0.01)
        assert result["reanimator"]["delver"]["match_count"] == 6

    def test_matchup_matrix_insufficient_data(self):
        """Test matchup matrix returns None for matchups with < 5 matches (Pioneer format)."""
        # Use different Pioneer archetypes with proper database columns
        df = pl.DataFrame({
            "player_archetype_id": [1, 1],
            "player_archetype": ["green_devotion", "green_devotion"],
            "opponent_archetype_id": [2, 2],
            "opponent_archetype": ["izzet_phoenix", "izzet_phoenix"],
            "player1_id": ["p1", "p2"],
            "player2_id": ["p3", "p4"],
            "winner_id": ["p1", "p4"],
        })

        service = MetaService()
        result = service._calculate_matchup_matrix(df, min_matches=5)

        # Only 2 matches, should return None for win_rate
        assert result["green_devotion"]["izzet_phoenix"]["win_rate"] is None
        assert result["green_devotion"]["izzet_phoenix"]["match_count"] == 2

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

    @patch("src.app.api.services.meta_analysis.DatabaseConnection")
    def test_fetch_archetype_data_for_format(self, mock_db):
        """Test fetching archetype data for a specific format (Pauper format)."""
        # Mock cursor and query results using Pauper archetypes
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "Pauper", "kuldotha_red", "red", "aggro", "2025-11-20"),
            (1, "Pauper", "kuldotha_red", "red", "aggro", "2025-11-19"),
            (2, "Pauper", "walls_combo", "bant", "combo", "2025-11-21"),
        ]
        mock_cursor.description = [
            ("archetype_group_id",), ("format",), ("main_title",),
            ("color_identity",), ("strategy",), ("tournament_date",)
        ]
        
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        service = MetaService()
        start_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
        end_date = datetime(2025, 11, 24, tzinfo=timezone.utc)
        
        result = service._fetch_archetype_data("Pauper", start_date, end_date)

        assert len(result) == 3
        assert "main_title" in result.columns
        assert "strategy" in result.columns
        assert result["main_title"].to_list() == ["kuldotha_red", "kuldotha_red", "walls_combo"]

    @patch("src.app.api.services.meta_analysis.DatabaseConnection")
    def test_fetch_match_data_for_format(self, mock_db):
        """Test fetching match data for a specific format (Modern format)."""
        # Use Modern archetypes different from previous tests
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "scam", 2, "hammer_time", 1, "2025-11-20"),
            (2, "hammer_time", 1, "scam", 2, "2025-11-21"),
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
        assert result["player_archetype"].to_list() == ["scam", "hammer_time"]

