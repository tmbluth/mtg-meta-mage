"""
Unit tests for MCP meta research tools.

Tests the meta_research_tools module which exposes format-wide meta analytics
as MCP tools.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
import polars as pl

from src.app.mcp.tools import meta_research_tools
from src.app.mcp.tools.meta_research_tools import (
    _calculate_time_windows,
    _calculate_meta_share,
    _calculate_win_rate,
    _calculate_matchup_matrix,
)

# Get the underlying functions from the MCP-wrapped versions
get_format_meta_rankings = meta_research_tools.get_format_meta_rankings.fn
get_format_matchup_stats = meta_research_tools.get_format_matchup_stats.fn


class TestGetFormatMetaRankings:
    """Test the get_format_meta_rankings MCP tool."""

    @patch("src.app.mcp.tools.meta_research_tools._fetch_archetype_data")
    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    def test_returns_rankings_with_metadata(self, mock_match_data, mock_archetype_data):
        """Test that get_format_meta_rankings returns properly formatted rankings."""
        # Setup mock data
        mock_archetype_data.return_value = pl.DataFrame({
            "archetype_group_id": [1, 2],
            "main_title": ["Deck A", "Deck B"],
            "color_identity": ["UR", "BG"],
            "strategy": ["combo", "midrange"],
            "format": ["Modern", "Modern"],
            "tournament_date": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
        })
        
        mock_match_data.return_value = pl.DataFrame({
            "player_archetype_id": [1, 1, 2],
            "player_archetype": ["Deck A", "Deck A", "Deck B"],
            "opponent_archetype_id": [2, 2, 1],
            "opponent_archetype": ["Deck B", "Deck B", "Deck A"],
            "player1_id": [100, 101, 102],
            "player2_id": [200, 201, 202],
            "winner_id": [100, 200, 102],
            "tournament_date": [datetime.now(timezone.utc)] * 3,
        })

        # Call the tool
        result = get_format_meta_rankings(
            format="Modern",
            current_days=14,
            previous_days=14,
        )

        # Verify structure
        assert "data" in result
        assert "metadata" in result
        assert isinstance(result["data"], list)
        assert isinstance(result["metadata"], dict)
        
        # Verify metadata
        assert result["metadata"]["format"] == "Modern"
        assert "current_period" in result["metadata"]
        assert "previous_period" in result["metadata"]
        assert "timestamp" in result["metadata"]

    @patch("src.app.mcp.tools.meta_research_tools._fetch_archetype_data")
    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    def test_handles_empty_data(self, mock_match_data, mock_archetype_data):
        """Test handling when no archetype data is available."""
        mock_archetype_data.return_value = pl.DataFrame(
            schema={
                "archetype_group_id": pl.Int64,
                "format": pl.Utf8,
                "main_title": pl.Utf8,
                "color_identity": pl.Utf8,
                "strategy": pl.Utf8,
                "tournament_date": pl.Datetime,
            }
        )
        mock_match_data.return_value = pl.DataFrame(
            schema={
                "player_archetype_id": pl.Int64,
                "player_archetype": pl.Utf8,
                "opponent_archetype_id": pl.Int64,
                "opponent_archetype": pl.Utf8,
                "player1_id": pl.Int64,
                "player2_id": pl.Int64,
                "winner_id": pl.Int64,
                "tournament_date": pl.Datetime,
            }
        )

        result = get_format_meta_rankings(format="Modern")
        
        assert result["data"] == []
        assert result["metadata"]["format"] == "Modern"


class TestGetFormatMatchupStats:
    """Test the get_format_matchup_stats MCP tool."""

    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    def test_returns_matchup_matrix(self, mock_match_data):
        """Test that get_format_matchup_stats returns properly formatted matrix."""
        mock_match_data.return_value = pl.DataFrame({
            "player_archetype": ["Deck A", "Deck A", "Deck B", "Deck B"],
            "opponent_archetype": ["Deck B", "Deck B", "Deck A", "Deck A"],
            "player1_id": [100, 101, 102, 103],
            "player2_id": [200, 201, 202, 203],
            "winner_id": [100, 200, 102, 202],
            "tournament_date": [datetime.now(timezone.utc)] * 4,
        })

        result = get_format_matchup_stats(format="Modern", days=14)

        assert "matrix" in result
        assert "archetypes" in result
        assert "metadata" in result
        assert isinstance(result["matrix"], dict)
        assert isinstance(result["archetypes"], list)

    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    def test_handles_empty_matches(self, mock_match_data):
        """Test handling when no match data is available."""
        mock_match_data.return_value = pl.DataFrame(
            schema={
                "player_archetype_id": pl.Int64,
                "player_archetype": pl.Utf8,
                "opponent_archetype_id": pl.Int64,
                "opponent_archetype": pl.Utf8,
                "player1_id": pl.Int64,
                "player2_id": pl.Int64,
                "winner_id": pl.Int64,
                "tournament_date": pl.Datetime,
            }
        )

        result = get_format_matchup_stats(format="Modern", days=14)

        assert result["matrix"] == {}
        assert result["archetypes"] == []


class TestHelperFunctions:
    """Test helper functions used by the MCP tools."""

    def test_calculate_time_windows(self):
        """Test time window calculation."""
        current_start, current_end, previous_start, previous_end = _calculate_time_windows(14, 14)
        
        # Verify time ordering
        assert previous_start < previous_end
        assert previous_end == current_start
        assert current_start < current_end
        
        # Verify day spans
        assert (current_end - current_start).days == 14
        assert (previous_end - previous_start).days == 14

    def test_calculate_meta_share_empty_dataframe(self):
        """Test meta share calculation with empty data."""
        df = pl.DataFrame(
            schema={
                "archetype_group_id": pl.Int64,
                "main_title": pl.Utf8,
                "color_identity": pl.Utf8,
                "strategy": pl.Utf8,
            }
        )
        
        result = _calculate_meta_share(df)
        assert len(result) == 0

    def test_calculate_win_rate_empty_dataframe(self):
        """Test win rate calculation with empty data."""
        df = pl.DataFrame(
            schema={
                "player_archetype_id": pl.Int64,
                "player_archetype": pl.Utf8,
                "player1_id": pl.Int64,
                "winner_id": pl.Int64,
            }
        )
        
        result = _calculate_win_rate(df)
        assert len(result) == 0

    def test_calculate_matchup_matrix_empty_dataframe(self):
        """Test matchup matrix calculation with empty data."""
        df = pl.DataFrame(
            schema={
                "player_archetype": pl.Utf8,
                "opponent_archetype": pl.Utf8,
                "player1_id": pl.Int64,
                "player2_id": pl.Int64,
                "winner_id": pl.Int64,
            }
        )
        
        result = _calculate_matchup_matrix(df)
        assert result == {}


class TestGetFormatArchetypes:
    """Tests for the get_format_archetypes MCP tool."""

    @patch("src.app.mcp.tools.meta_research_tools.DatabaseConnection.get_cursor")
    def test_returns_sorted_archetypes_with_meta_share(self, mock_get_cursor):
        """Valid format returns archetypes sorted by meta_share with required schema."""
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (1, "Deck A", "UR", 10),
            (2, "Deck B", "BG", 5),
        ]
        cursor.description = [
            ("archetype_group_id",),
            ("main_title",),
            ("color_identity",),
            ("deck_count",),
        ]
        mock_get_cursor.return_value.__enter__.return_value = cursor

        result = meta_research_tools.get_format_archetypes.fn(format="Modern", days=30)

        assert result["format"] == "Modern"
        archetypes = result["archetypes"]
        assert len(archetypes) == 2
        assert archetypes[0]["name"] == "Deck A"
        assert archetypes[0]["meta_share"] == pytest.approx(66.666, rel=1e-2)
        assert archetypes[0]["color_identity"] == "UR"
        # Ensure sorted by meta_share descending
        assert archetypes[0]["meta_share"] >= archetypes[1]["meta_share"]

    @patch("src.app.mcp.tools.meta_research_tools.DatabaseConnection.get_cursor")
    def test_handles_no_archetype_data(self, mock_get_cursor):
        """No data returns empty archetypes array and echoes format."""
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.description = [
            ("archetype_group_id",),
            ("main_title",),
            ("color_identity",),
            ("deck_count",),
        ]
        mock_get_cursor.return_value.__enter__.return_value = cursor

        result = meta_research_tools.get_format_archetypes.fn(format="Legacy", days=30)

        assert result["format"] == "Legacy"
        assert result["archetypes"] == []

