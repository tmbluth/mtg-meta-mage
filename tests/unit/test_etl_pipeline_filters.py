"""Unit tests for ETL pipeline filter functions"""

import pytest

from src.etl.etl_pipeline import (
    is_commander_format,
    is_limited_format,
    should_include_tournament,
    is_valid_match,
    filter_tournaments,
    filter_rounds_data,
    COMMANDER_FORMATS,
    LIMITED_FORMATS
)
from tests.unit.test_etl_pipeline_helpers import (
    create_mock_tournament,
    create_mock_table,
    create_mock_round
)


class TestIsCommanderFormat:
    """Tests for is_commander_format function"""
    
    def test_identifies_commander_formats(self):
        """Test that all commander formats are correctly identified"""
        for format_name in COMMANDER_FORMATS:
            assert is_commander_format(format_name) is True, f"Failed to identify {format_name} as commander format"
    
    @pytest.mark.parametrize("format_name", [
        'Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper'
    ])
    def test_identifies_non_commander_formats(self, format_name):
        """Test that non-commander formats are not incorrectly identified"""
        assert is_commander_format(format_name) is False, f"Incorrectly identified {format_name} as commander format"
    
    @pytest.mark.parametrize("value,expected", [
        ('', False),
        (None, False),
        ('  EDH  ', True),
        ('  Standard  ', False),
        ('edh', False),  # lowercase should not match
        ('EDH', True)
    ])
    def test_handles_edge_cases(self, value, expected):
        """Test edge cases including empty, None, whitespace, and case sensitivity"""
        assert is_commander_format(value) == expected


class TestIsLimitedFormat:
    """Tests for is_limited_format function"""
    
    def test_identifies_limited_formats(self):
        """Test that all limited formats are correctly identified"""
        for format_name in LIMITED_FORMATS:
            assert is_limited_format(format_name) is True, f"Failed to identify {format_name} as limited format"
    
    @pytest.mark.parametrize("format_name", [
        'Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper', 'EDH'
    ])
    def test_identifies_non_limited_formats(self, format_name):
        """Test that non-limited formats are not incorrectly identified"""
        assert is_limited_format(format_name) is False, f"Incorrectly identified {format_name} as limited format"
    
    @pytest.mark.parametrize("value,expected", [
        ('', False),
        (None, False),
        ('  Draft  ', True),
        ('  Standard  ', False),
        ('draft', False),  # lowercase should not match
        ('Draft', True)
    ])
    def test_handles_edge_cases(self, value, expected):
        """Test edge cases including empty, None, whitespace, and case sensitivity"""
        assert is_limited_format(value) == expected


class TestShouldIncludeTournament:
    """Tests for should_include_tournament function"""
    
    def test_includes_standard_mtg_tournament(self):
        """Test that valid MTG tournaments are included"""
        tournament = create_mock_tournament(format='Standard', game='Magic: The Gathering')
        assert should_include_tournament(tournament) is True
    
    @pytest.mark.parametrize("format_name", list(COMMANDER_FORMATS))
    def test_excludes_all_commander_formats(self, format_name):
        """Test that all commander formats are excluded"""
        tournament = create_mock_tournament(format=format_name, game='Magic: The Gathering')
        assert should_include_tournament(tournament) is False, f"Failed to exclude {format_name}"
    
    @pytest.mark.parametrize("format_name", list(LIMITED_FORMATS))
    def test_excludes_all_limited_formats(self, format_name):
        """Test that all limited formats are excluded"""
        tournament = create_mock_tournament(format=format_name, game='Magic: The Gathering')
        assert should_include_tournament(tournament) is False, f"Failed to exclude {format_name}"
    
    def test_excludes_non_mtg_game(self):
        """Test that non-MTG games are excluded"""
        tournament = create_mock_tournament(format='Standard', game='Pokemon')
        assert should_include_tournament(tournament) is False
    
    def test_handles_missing_format(self):
        """Test that tournaments with missing format are included if MTG"""
        tournament = create_mock_tournament(game='Magic: The Gathering')
        del tournament['format']
        assert should_include_tournament(tournament) is True
    
    def test_handles_missing_game(self):
        """Test that tournaments with missing game field are excluded"""
        tournament = create_mock_tournament(format='Standard')
        del tournament['game']
        assert should_include_tournament(tournament) is False


class TestIsValidMatch:
    """Tests for is_valid_match function"""
    
    @pytest.mark.parametrize("player_count,expected", [
        (0, True),   # Empty match (possible in data)
        (1, True),   # Bye
        (2, True),   # Normal match
        (3, False),  # Multiplayer - invalid
        (4, False),  # Multiplayer - invalid
        (5, False)   # Multiplayer - invalid
    ])
    def test_validates_player_count(self, player_count, expected):
        """Test that matches are validated based on player count"""
        players = [{'id': f'p{i}', 'name': f'Player{i}'} for i in range(player_count)]
        table_data = create_mock_table(players=players)
        assert is_valid_match(table_data) == expected
    
    def test_handles_missing_players_key(self):
        """Test that missing players key returns True (empty list default)"""
        table_data = {'table': 1}
        assert is_valid_match(table_data) is True


class TestFilterTournaments:
    """Tests for filter_tournaments function"""
    
    def test_filters_out_commander_tournaments(self):
        """Test that commander tournaments are filtered out"""
        tournaments = [
            create_mock_tournament(TID='1', format='Standard'),
            create_mock_tournament(TID='2', format='EDH'),
            create_mock_tournament(TID='3', format='Modern'),
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
    
    def test_filters_out_limited_tournaments(self):
        """Test that limited tournaments are filtered out"""
        tournaments = [
            create_mock_tournament(TID='1', format='Standard'),
            create_mock_tournament(TID='2', format='Draft'),
            create_mock_tournament(TID='3', format='Modern'),
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_both_commander_and_limited(self):
        """Test that both commander and limited tournaments are filtered out"""
        tournaments = [
            create_mock_tournament(TID='1', format='Standard'),
            create_mock_tournament(TID='2', format='EDH'),
            create_mock_tournament(TID='3', format='Draft'),
            create_mock_tournament(TID='4', format='Sealed'),
            create_mock_tournament(TID='5', format='Modern'),
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_non_mtg_games(self):
        """Test that non-MTG games are filtered out"""
        tournaments = [
            create_mock_tournament(TID='1', format='Standard', game='Magic: The Gathering'),
            create_mock_tournament(TID='2', format='Standard', game='Pokemon'),
            create_mock_tournament(TID='3', format='Modern', game='Magic: The Gathering'),
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['game'] == 'Magic: The Gathering' for t in filtered)
    
    def test_returns_empty_list_for_empty_input(self):
        """Test that empty input returns empty list"""
        assert filter_tournaments([]) == []
    
    def test_keeps_all_valid_tournaments(self):
        """Test that all valid tournaments are kept"""
        tournaments = [
            create_mock_tournament(TID='1', format='Standard'),
            create_mock_tournament(TID='2', format='Modern'),
            create_mock_tournament(TID='3', format='Legacy'),
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 3


class TestFilterRoundsData:
    """Tests for filter_rounds_data function"""
    
    def test_filters_multiplayer_matches(self):
        """Test that multiplayer matches are filtered out"""
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': '1'}, {'id': '2'}]),  # valid
                    create_mock_table(table=2, players=[{'id': '3'}, {'id': '4'}, {'id': '5'}]),  # invalid
                ]
            )
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert len(filtered[0]['tables']) == 1
        assert filtered[0]['tables'][0]['table'] == 1
    
    def test_removes_rounds_with_no_valid_matches(self):
        """Test that rounds with no valid matches are removed"""
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': '1'}, {'id': '2'}, {'id': '3'}]),
                    create_mock_table(table=2, players=[{'id': '4'}, {'id': '5'}, {'id': '6'}]),
                ]
            )
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 0
    
    def test_keeps_all_valid_matches(self):
        """Test that all valid matches are kept"""
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': '1'}, {'id': '2'}]),
                    create_mock_table(table=2, players=[{'id': '3'}, {'id': '4'}]),
                ]
            )
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert len(filtered[0]['tables']) == 2
    
    def test_handles_empty_input(self):
        """Test that empty input returns empty list"""
        assert filter_rounds_data([]) == []
    
    def test_handles_rounds_with_empty_tables(self):
        """Test that rounds with empty tables list are removed"""
        rounds_data = [
            create_mock_round(round=1, tables=[]),
            create_mock_round(
                round=2,
                tables=[create_mock_table(table=1, players=[{'id': '1'}, {'id': '2'}])]
            )
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert filtered[0]['round'] == 2
    
    def test_preserves_round_data_structure(self):
        """Test that round data structure is preserved"""
        rounds_data = [
            {
                'round': 1,
                'extra_field': 'test_value',
                'tables': [
                    create_mock_table(table=1, players=[{'id': '1'}, {'id': '2'}], status='complete')
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert filtered[0]['round'] == 1
        assert filtered[0]['extra_field'] == 'test_value'
        assert filtered[0]['tables'][0]['status'] == 'complete'

