"""
Meta Analytics Service - Business logic for meta analytics calculations.

This module provides analytics calculations for archetype performance and matchups
using polars for efficient data aggregation.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl

from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class MetaService:
    """Service for calculating meta analytics from tournament data."""

    def __init__(self):
        """Initialize the meta service."""
        pass

    def get_archetype_rankings(
        self,
        format: str,
        current_days: int = 14,
        previous_days: int = 14,
        color_identity: Optional[str] = None,
        strategy: Optional[str] = None,
        group_by: Optional[str] = None,
    ) -> dict:
        """
        Calculate archetype rankings with meta share and win rate for a format.

        Args:
            format: Tournament format (e.g., "Modern", "Pioneer")
            current_days: Number of days back from today for current period (default: 14)
            previous_days: Number of days back from end of current period for previous period (default: 14)
            color_identity: Optional filter by color identity
            strategy: Optional filter by strategy
            group_by: Optional grouping field (color_identity or strategy)

        Returns:
            Dictionary with archetype rankings data and metadata
        """
        # Calculate time windows
        current_start, current_end, previous_start, previous_end = self._calculate_time_windows(
            current_days, previous_days
        )

        # Fetch data for both periods
        current_archetype_data = self._fetch_archetype_data(format, current_start, current_end)
        previous_archetype_data = self._fetch_archetype_data(format, previous_start, previous_end)

        current_match_data = self._fetch_match_data(format, current_start, current_end)
        previous_match_data = self._fetch_match_data(format, previous_start, previous_end)

        # Calculate meta share and win rates
        current_meta = self._calculate_meta_share(current_archetype_data)
        previous_meta = self._calculate_meta_share(previous_archetype_data)

        current_wins = self._calculate_win_rate(current_match_data)
        previous_wins = self._calculate_win_rate(previous_match_data)

        # Merge current and previous data
        result = self._merge_period_data(current_meta, previous_meta, current_wins, previous_wins)

        # Apply filters
        if color_identity:
            result = self._filter_by_color_identity(result, color_identity)
        if strategy:
            result = self._filter_by_strategy(result, strategy)

        # Apply grouping
        if group_by == "color_identity":
            result = self._group_by_color_identity(result)
        elif group_by == "strategy":
            result = self._group_by_strategy(result)

        # Sort by current meta share descending
        result = result.sort("meta_share_current", descending=True)

        # Convert to dictionary format
        data = result.to_dicts()

        metadata = {
            "format": format,
            "current_period": {
                "days": current_days,
                "start_date": current_start.isoformat(),
                "end_date": current_end.isoformat(),
            },
            "previous_period": {
                "days": previous_days,
                "start_date": previous_start.isoformat(),
                "end_date": previous_end.isoformat(),
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return {"data": data, "metadata": metadata}

    def get_matchup_matrix(
        self,
        format: str,
        days: int = 14,
    ) -> dict:
        """
        Calculate matchup matrix showing head-to-head win rates between archetypes.

        Args:
            format: Tournament format (e.g., "Modern", "Pioneer")
            days: Number of days to include in analysis (default: 14)

        Returns:
            Dictionary with matchup matrix and metadata
        """
        # Calculate time window
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        end_date = datetime.now(timezone.utc)

        # Fetch match data
        match_data = self._fetch_match_data(format, start_date, end_date)

        if len(match_data) == 0:
            return {
                "matrix": {},
                "archetypes": [],
                "metadata": {
                    "format": format,
                    "days": days,
                    "start_date": start_date.isoformat(),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            }

        # Calculate matchup matrix
        matrix = self._calculate_matchup_matrix(match_data)

        # Get list of all archetypes
        archetypes = sorted(list(matrix.keys()))

        metadata = {
            "format": format,
            "days": days,
            "start_date": start_date.isoformat(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return {"matrix": matrix, "archetypes": archetypes, "metadata": metadata}

    def _calculate_time_windows(
        self, current_days: int, previous_days: int
    ) -> tuple[datetime, datetime, datetime, datetime]:
        """
        Calculate start and end dates for current and previous time windows.

        Args:
            current_days: Number of days back from today for current period
            previous_days: Number of days back from end of current period for previous period

        Returns:
            Tuple of (current_start, current_end, previous_start, previous_end)
        """
        now = datetime.now(timezone.utc)
        current_end = now
        current_start = now - timedelta(days=current_days)
        previous_end = current_start
        previous_start = current_start - timedelta(days=previous_days)

        return current_start, current_end, previous_start, previous_end

    def _fetch_archetype_data(self, format: str, start_date: datetime, end_date: datetime) -> pl.DataFrame:
        """
        Fetch archetype data from database for a given format and time window.

        Args:
            format: Tournament format
            start_date: Start of time window
            end_date: End of time window

        Returns:
            Polars DataFrame with archetype data
        """
        query = """
            SELECT 
                ag.archetype_group_id,
                ag.format,
                ag.main_title,
                ag.color_identity,
                ag.strategy,
                t.start_date as tournament_date
            FROM decklists d
            JOIN archetype_groups ag ON d.archetype_group_id = ag.archetype_group_id
            JOIN tournaments t ON d.tournament_id = t.tournament_id
            WHERE ag.format = %s
              AND t.start_date >= %s
              AND t.start_date < %s
        """

        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(query, (format, start_date, end_date))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

        if not rows:
            return pl.DataFrame(
                schema={
                    "archetype_group_id": pl.Int64,
                    "format": pl.Utf8,
                    "main_title": pl.Utf8,
                    "color_identity": pl.Utf8,
                    "strategy": pl.Utf8,
                    "tournament_date": pl.Datetime,
                }
            )

        return pl.DataFrame(rows, schema=columns, orient="row")

    def _fetch_match_data(self, format: str, start_date: datetime, end_date: datetime) -> pl.DataFrame:
        """
        Fetch match data from database for a given format and time window.

        Args:
            format: Tournament format
            start_date: Start of time window
            end_date: End of time window

        Returns:
            Polars DataFrame with match data
        """
        query = """
            SELECT 
                ag1.archetype_group_id as player_archetype_id,
                ag1.main_title as player_archetype,
                ag2.archetype_group_id as opponent_archetype_id,
                ag2.main_title as opponent_archetype,
                m.player1_id,
                m.player2_id,
                m.winner_id,
                t.start_date as tournament_date
            FROM matches m
            JOIN tournaments t ON m.tournament_id = t.tournament_id
            JOIN decklists d1 ON m.player1_id = d1.player_id AND m.tournament_id = d1.tournament_id
            JOIN decklists d2 ON m.player2_id = d2.player_id AND m.tournament_id = d2.tournament_id
            JOIN archetype_groups ag1 ON d1.archetype_group_id = ag1.archetype_group_id
            JOIN archetype_groups ag2 ON d2.archetype_group_id = ag2.archetype_group_id
            WHERE t.format = %s
              AND t.start_date >= %s
              AND t.start_date < %s
              AND m.winner_id IS NOT NULL
        """

        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(query, (format, start_date, end_date))
            rows = cursor.fetchall()
            if not rows:
                return pl.DataFrame(
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
            columns = [desc[0] for desc in cursor.description]

        return pl.DataFrame(rows, schema=columns, orient="row")

    def _calculate_meta_share(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate meta share percentage for each archetype.

        Args:
            df: DataFrame with archetype data

        Returns:
            DataFrame with meta share calculations
        """
        if len(df) == 0:
            return pl.DataFrame(
                schema={
                    "archetype_group_id": pl.Int64,
                    "main_title": pl.Utf8,
                    "color_identity": pl.Utf8,
                    "strategy": pl.Utf8,
                    "meta_share": pl.Float64,
                    "sample_size": pl.Int64,
                }
            )

        total_decklists = len(df)

        # Group by archetype and calculate counts
        result = (
            df.group_by(["archetype_group_id", "main_title", "color_identity", "strategy"])
            .agg([pl.len().alias("sample_size")])
            .with_columns([(pl.col("sample_size") / total_decklists * 100).alias("meta_share")])
            .sort("meta_share", descending=True)
        )

        return result

    def _calculate_win_rate(self, df: pl.DataFrame, min_matches: int = 3) -> pl.DataFrame:
        """
        Calculate win rate for each archetype from match data.

        Args:
            df: DataFrame with match data (must have player_archetype_id, player1_id, winner_id)
            min_matches: Minimum number of matches required for valid win rate

        Returns:
            DataFrame with win rate calculations
        """
        if len(df) == 0:
            return pl.DataFrame(
                schema={
                    "archetype_group_id": pl.Int64,
                    "main_title": pl.Utf8,
                    "win_rate": pl.Float64,
                    "match_count": pl.Int64,
                }
            )

        # Create separate DataFrames for each player's perspective
        # Player 1 matches
        p1_data = df.select([
            pl.col("player_archetype_id").alias("archetype_group_id"),
            pl.col("player_archetype").alias("main_title"),
            (pl.col("winner_id") == pl.col("player1_id")).alias("is_win"),
        ])

        # Player 2 matches (need to swap perspectives)
        p2_data = df.select([
            pl.col("opponent_archetype_id").alias("archetype_group_id"),
            pl.col("opponent_archetype").alias("main_title"),
            (pl.col("winner_id") == pl.col("player2_id")).alias("is_win"),
        ])

        # Combine both perspectives
        all_matches = pl.concat([p1_data, p2_data])

        # Calculate win rate
        result = (
            all_matches.group_by(["archetype_group_id", "main_title"])
            .agg([
                pl.col("is_win").sum().alias("wins"),
                pl.len().alias("match_count"),
            ])
            .with_columns([
                pl.when(pl.col("match_count") >= min_matches)
                .then((pl.col("wins") / pl.col("match_count") * 100))
                .otherwise(None)
                .alias("win_rate")
            ])
        )

        return result

    def _merge_period_data(
        self,
        current_meta: pl.DataFrame,
        previous_meta: pl.DataFrame,
        current_wins: pl.DataFrame,
        previous_wins: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Merge current and previous period data into single DataFrame.

        Args:
            current_meta: Current period meta share data
            previous_meta: Previous period meta share data
            current_wins: Current period win rate data
            previous_wins: Previous period win rate data

        Returns:
            Merged DataFrame with current and previous metrics
        """
        # Start with current meta as base
        result = current_meta.select([
            "archetype_group_id",
            "main_title",
            "color_identity",
            "strategy",
            pl.col("meta_share").alias("meta_share_current"),
            pl.col("sample_size").alias("sample_size_current"),
        ])

        # Join previous meta
        if len(previous_meta) > 0:
            previous_meta_renamed = previous_meta.select([
                "archetype_group_id",
                pl.col("meta_share").alias("meta_share_previous"),
                pl.col("sample_size").alias("sample_size_previous"),
            ])
            result = result.join(
                previous_meta_renamed, on="archetype_group_id", how="left"
            )
        else:
            result = result.with_columns([
                pl.lit(None, dtype=pl.Float64).alias("meta_share_previous"),
                pl.lit(None, dtype=pl.Int64).alias("sample_size_previous"),
            ])

        # Join current win rates
        if len(current_wins) > 0:
            current_wins_renamed = current_wins.select([
                "archetype_group_id",
                pl.col("win_rate").alias("win_rate_current"),
                pl.col("match_count").alias("match_count_current"),
            ])
            result = result.join(
                current_wins_renamed, on="archetype_group_id", how="left"
            )
        else:
            result = result.with_columns([
                pl.lit(None, dtype=pl.Float64).alias("win_rate_current"),
                pl.lit(None, dtype=pl.Int64).alias("match_count_current"),
            ])

        # Join previous win rates
        if len(previous_wins) > 0:
            previous_wins_renamed = previous_wins.select([
                "archetype_group_id",
                pl.col("win_rate").alias("win_rate_previous"),
                pl.col("match_count").alias("match_count_previous"),
            ])
            result = result.join(
                previous_wins_renamed, on="archetype_group_id", how="left"
            )
        else:
            result = result.with_columns([
                pl.lit(None, dtype=pl.Float64).alias("win_rate_previous"),
                pl.lit(None, dtype=pl.Int64).alias("match_count_previous"),
            ])

        return result

    def _filter_by_color_identity(self, df: pl.DataFrame, color_identity: str) -> pl.DataFrame:
        """Filter DataFrame by color identity."""
        return df.filter(pl.col("color_identity") == color_identity)

    def _filter_by_strategy(self, df: pl.DataFrame, strategy: str) -> pl.DataFrame:
        """Filter DataFrame by strategy."""
        return df.filter(pl.col("strategy") == strategy)

    def _group_by_color_identity(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Group archetypes by color identity and aggregate metrics.

        Args:
            df: DataFrame with archetype data

        Returns:
            DataFrame grouped by color_identity
        """
        grouped = df.group_by("color_identity").agg([
            pl.col("meta_share_current").sum().alias("meta_share_current"),
            pl.col("sample_size_current").sum().alias("sample_size_current"),
            pl.col("meta_share_previous").sum().alias("meta_share_previous"),
            pl.col("sample_size_previous").sum().alias("sample_size_previous"),
            pl.col("win_rate_current").mean().alias("win_rate_current"),
            pl.col("win_rate_previous").mean().alias("win_rate_previous"),
            pl.col("match_count_current").sum().alias("match_count_current"),
            pl.col("match_count_previous").sum().alias("match_count_previous"),
        ])
        # Add placeholder for required fields that were dropped during grouping
        return grouped.with_columns([
            pl.lit("grouped").alias("main_title"),
            pl.col("color_identity").alias("strategy"),  # Use color_identity as strategy placeholder
        ])

    def _group_by_strategy(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Group archetypes by strategy and aggregate metrics.

        Args:
            df: DataFrame with archetype data

        Returns:
            DataFrame grouped by strategy
        """
        grouped = df.group_by("strategy").agg([
            pl.col("meta_share_current").sum().alias("meta_share_current"),
            pl.col("sample_size_current").sum().alias("sample_size_current"),
            pl.col("meta_share_previous").sum().alias("meta_share_previous"),
            pl.col("sample_size_previous").sum().alias("sample_size_previous"),
            pl.col("win_rate_current").mean().alias("win_rate_current"),
            pl.col("win_rate_previous").mean().alias("win_rate_previous"),
            pl.col("match_count_current").sum().alias("match_count_current"),
            pl.col("match_count_previous").sum().alias("match_count_previous"),
        ])
        # Add placeholder for required fields that were dropped during grouping
        return grouped.with_columns([
            pl.lit("grouped").alias("main_title"),
            pl.col("strategy").alias("color_identity"),  # Use strategy as color_identity placeholder
        ])

    def _calculate_matchup_matrix(self, df: pl.DataFrame, min_matches: int = 3) -> dict:
        """
        Calculate matchup matrix from match data.

        Args:
            df: DataFrame with match data
            min_matches: Minimum number of matches required for valid win rate

        Returns:
            Nested dictionary: {player_archetype: {opponent_archetype: {win_rate, match_count}}}
        """
        if len(df) == 0:
            return {}

        # Create matchup data for both player perspectives
        # Player 1 perspective
        p1_matchups = df.select([
            pl.col("player_archetype"),
            pl.col("opponent_archetype"),
            (pl.col("winner_id") == pl.col("player1_id")).alias("is_win"),
        ])

        # Player 2 perspective (swap player and opponent)
        p2_matchups = df.select([
            pl.col("opponent_archetype").alias("player_archetype"),
            pl.col("player_archetype").alias("opponent_archetype"),
            (pl.col("winner_id") == pl.col("player2_id")).alias("is_win"),
        ])

        # Combine both perspectives
        all_matchups = pl.concat([p1_matchups, p2_matchups])

        # Calculate win rates for each matchup
        matchup_stats = (
            all_matchups.group_by(["player_archetype", "opponent_archetype"])
            .agg([
                pl.col("is_win").sum().alias("wins"),
                pl.len().alias("match_count"),
            ])
            .with_columns([
                pl.when(pl.col("match_count") >= min_matches)
                .then((pl.col("wins") / pl.col("match_count") * 100))
                .otherwise(None)
                .alias("win_rate")
            ])
        )

        # Convert to nested dictionary format
        matrix = {}
        for row in matchup_stats.iter_rows(named=True):
            player = row["player_archetype"]
            opponent = row["opponent_archetype"]

            if player not in matrix:
                matrix[player] = {}

            matrix[player][opponent] = {
                "win_rate": row["win_rate"],
                "match_count": row["match_count"],
            }

        return matrix

