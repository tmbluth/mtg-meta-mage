"""Unit tests for data filtering logic"""

import pytest
from src.data.filters import (
    is_commander_format,
    is_limited_format,
    should_include_tournament,
    is_valid_match,
    filter_tournaments,
    filter_rounds_data,
    COMMANDER_FORMATS,
    LIMITED_FORMATS
)


class TestIsCommanderFormat:
    """Tests for is_commander_format function"""
    
    def test_identifies_commander_formats(self):
        """Test that all commander formats are correctly identified"""
        for format_name in COMMANDER_FORMATS:
            assert is_commander_format(format_name) is True, f"Failed to identify {format_name} as commander format"
    
    def test_identifies_non_commander_formats(self):
        """Test that non-commander formats are not incorrectly identified"""
        non_commander_formats = ['Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper']
        for format_name in non_commander_formats:
            assert is_commander_format(format_name) is False, f"Incorrectly identified {format_name} as commander format"
    
    def test_handles_empty_string(self):
        """Test that empty string returns False"""
        assert is_commander_format('') is False
    
    def test_handles_none(self):
        """Test that None returns False"""
        assert is_commander_format(None) is False
    
    def test_handles_whitespace(self):
        """Test that whitespace is stripped correctly"""
        assert is_commander_format('  EDH  ') is True
        assert is_commander_format('  Standard  ') is False
    
    def test_case_sensitivity(self):
        """Test that format checking is case sensitive"""
        assert is_commander_format('edh') is False  # lowercase should not match
        assert is_commander_format('EDH') is True


class TestIsLimitedFormat:
    """Tests for is_limited_format function"""
    
    def test_identifies_limited_formats(self):
        """Test that all limited formats are correctly identified"""
        for format_name in LIMITED_FORMATS:
            assert is_limited_format(format_name) is True, f"Failed to identify {format_name} as limited format"
    
    def test_identifies_non_limited_formats(self):
        """Test that non-limited formats are not incorrectly identified"""
        non_limited_formats = ['Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 'Pauper', 'EDH']
        for format_name in non_limited_formats:
            assert is_limited_format(format_name) is False, f"Incorrectly identified {format_name} as limited format"
    
    def test_handles_empty_string(self):
        """Test that empty string returns False"""
        assert is_limited_format('') is False
    
    def test_handles_none(self):
        """Test that None returns False"""
        assert is_limited_format(None) is False
    
    def test_handles_whitespace(self):
        """Test that whitespace is stripped correctly"""
        assert is_limited_format('  Draft  ') is True
        assert is_limited_format('  Standard  ') is False
    
    def test_case_sensitivity(self):
        """Test that format checking is case sensitive"""
        assert is_limited_format('draft') is False  # lowercase should not match
        assert is_limited_format('Draft') is True


class TestShouldIncludeTournament:
    """Tests for should_include_tournament function"""
    
    def test_includes_standard_mtg_tournament(self):
        """Test that valid MTG tournaments are included"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is True
    
    def test_excludes_commander_tournament(self):
        """Test that commander tournaments are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'EDH',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_non_mtg_game(self):
        """Test that non-MTG games are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard',
            'game': 'Pokemon'
        }
        assert should_include_tournament(tournament) is False
    
    def test_handles_missing_format(self):
        """Test that tournaments with missing format are included if MTG"""
        tournament = {
            'TID': 'test123',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is True
    
    def test_handles_missing_game(self):
        """Test that tournaments with missing game field are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Standard'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_all_commander_formats(self):
        """Test that all commander formats are excluded"""
        for cmd_format in COMMANDER_FORMATS:
            tournament = {
                'TID': 'test123',
                'format': cmd_format,
                'game': 'Magic: The Gathering'
            }
            assert should_include_tournament(tournament) is False, f"Failed to exclude {cmd_format}"
    
    def test_excludes_limited_tournament(self):
        """Test that limited tournaments are excluded"""
        tournament = {
            'TID': 'test123',
            'format': 'Draft',
            'game': 'Magic: The Gathering'
        }
        assert should_include_tournament(tournament) is False
    
    def test_excludes_all_limited_formats(self):
        """Test that all limited formats are excluded"""
        for limited_format in LIMITED_FORMATS:
            tournament = {
                'TID': 'test123',
                'format': limited_format,
                'game': 'Magic: The Gathering'
            }
            assert should_include_tournament(tournament) is False, f"Failed to exclude {limited_format}"


class TestIsValidMatch:
    """Tests for is_valid_match function"""
    
    def test_valid_two_player_match(self):
        """Test that a valid 2-player match is accepted"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'},
                {'id': 'player2', 'name': 'Bob'}
            ]
        }
        assert is_valid_match(table_data) is True
    
    def test_valid_one_player_match(self):
        """Test that a 1-player match (bye) is accepted"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'}
            ]
        }
        assert is_valid_match(table_data) is True
    
    def test_invalid_three_player_match(self):
        """Test that a 3-player match is rejected"""
        table_data = {
            'table': 1,
            'players': [
                {'id': 'player1', 'name': 'Alice'},
                {'id': 'player2', 'name': 'Bob'},
                {'id': 'player3', 'name': 'Charlie'}
            ]
        }
        assert is_valid_match(table_data) is False
    
    def test_invalid_four_player_match(self):
        """Test that a 4-player match is rejected"""
        table_data = {
            'table': 1,
            'players': [
                {'id': f'player{i}', 'name': f'Player{i}'} for i in range(4)
            ]
        }
        assert is_valid_match(table_data) is False
    
    def test_handles_empty_players_list(self):
        """Test that empty players list is accepted"""
        table_data = {
            'table': 1,
            'players': []
        }
        assert is_valid_match(table_data) is True
    
    def test_handles_missing_players_key(self):
        """Test that missing players key returns empty list (True)"""
        table_data = {
            'table': 1
        }
        assert is_valid_match(table_data) is True


class TestFilterTournaments:
    """Tests for filter_tournaments function"""
    
    def test_filters_out_commander_tournaments(self):
        """Test that commander tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'EDH', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
    
    def test_filters_out_limited_tournaments(self):
        """Test that limited tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Draft', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_both_commander_and_limited(self):
        """Test that both commander and limited tournaments are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'EDH', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Draft', 'game': 'Magic: The Gathering'},
            {'TID': '4', 'format': 'Sealed', 'game': 'Magic: The Gathering'},
            {'TID': '5', 'format': 'Modern', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 2
        assert all(t['format'] not in COMMANDER_FORMATS for t in filtered)
        assert all(t['format'] not in LIMITED_FORMATS for t in filtered)
    
    def test_filters_out_non_mtg_games(self):
        """Test that non-MTG games are filtered out"""
        tournaments = [
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Standard', 'game': 'Pokemon'},
            {'TID': '3', 'format': 'Modern', 'game': 'Magic: The Gathering'},
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
            {'TID': '1', 'format': 'Standard', 'game': 'Magic: The Gathering'},
            {'TID': '2', 'format': 'Modern', 'game': 'Magic: The Gathering'},
            {'TID': '3', 'format': 'Legacy', 'game': 'Magic: The Gathering'},
        ]
        filtered = filter_tournaments(tournaments)
        assert len(filtered) == 3


class TestFilterRoundsData:
    """Tests for filter_rounds_data function"""
    
    def test_filters_multiplayer_matches(self):
        """Test that multiplayer matches are filtered out"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}]},  # valid
                    {'table': 2, 'players': [{'id': '3'}, {'id': '4'}, {'id': '5'}]},  # invalid
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 1
        assert len(filtered[0]['tables']) == 1
        assert filtered[0]['tables'][0]['table'] == 1
    
    def test_removes_rounds_with_no_valid_matches(self):
        """Test that rounds with no valid matches are removed"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}, {'id': '3'}]},
                    {'table': 2, 'players': [{'id': '4'}, {'id': '5'}, {'id': '6'}]},
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert len(filtered) == 0
    
    def test_keeps_all_valid_matches(self):
        """Test that all valid matches are kept"""
        rounds_data = [
            {
                'round': 1,
                'tables': [
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}]},
                    {'table': 2, 'players': [{'id': '3'}, {'id': '4'}]},
                ]
            }
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
            {'round': 1, 'tables': []},
            {'round': 2, 'tables': [{'table': 1, 'players': [{'id': '1'}, {'id': '2'}]}]}
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
                    {'table': 1, 'players': [{'id': '1'}, {'id': '2'}], 'status': 'complete'}
                ]
            }
        ]
        filtered = filter_rounds_data(rounds_data)
        assert filtered[0]['round'] == 1
        assert filtered[0]['extra_field'] == 'test_value'
        assert filtered[0]['tables'][0]['status'] == 'complete'

