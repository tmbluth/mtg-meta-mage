"""Unit tests for TopDeck-related ETL pipeline functions"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call

from src.etl.etl_pipeline import ETLPipeline, parse_decklist
from tests.unit.test_etl_pipeline_helpers import (
    create_mock_tournament,
    create_mock_player,
    create_mock_round,
    create_mock_table,
    create_mock_decklist_entry
)


class TestParseDecklistStandardFormat:
    """Tests for parsing standard MTG decklist format (quantity + card name)"""
    
    @pytest.mark.parametrize("decklist,expected_length,expected_cards", [
        ("4 Lightning Bolt\n2 Mountain\n1 Island", 3, [
            {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
            {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'},
            {'quantity': 1, 'card_name': 'Island', 'section': 'mainboard'}
        ]),
        ("4\tLightning Bolt\n2\tMountain", 2, [
            {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
            {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
        ]),
        ("4   Lightning Bolt\n2    Mountain", 2, [
            {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'},
            {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
        ])
    ])
    def test_parse_simple_decklist_formats(self, decklist, expected_length, expected_cards):
        """Test parsing simple decklists with various spacing formats"""
        result = parse_decklist(decklist)
        assert len(result) == expected_length
        assert result == expected_cards
    
    def test_parse_decklist_with_card_names_containing_numbers(self):
        """Test parsing cards with numbers in their names"""
        decklist = "4 Lightning Bolt\n2 Mountain\n1 Sol Ring"
        result = parse_decklist(decklist)
        
        assert len(result) == 3
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
        assert result[2]['card_name'] == 'Sol Ring'


class TestParseDecklistMainboardSideboard:
    """Tests for identifying mainboard vs sideboard sections"""
    
    @pytest.mark.parametrize("decklist,mainboard_count,sideboard_count", [
        ("4 Lightning Bolt\n2 Mountain\n\nSideboard\n2 Counterspell\n1 Negate", 2, 2),
        ("4 Lightning Bolt\nSideboard:\n2 Counterspell", 1, 1),
        ("4 Lightning Bolt\nSB: 2 Counterspell", 1, 1),
        ("4 Lightning Bolt\n// Sideboard\n2 Counterspell", 1, 1),
    ])
    def test_parse_decklist_with_sideboard_separators(self, decklist, mainboard_count, sideboard_count):
        """Test parsing decklist with various sideboard separators"""
        result = parse_decklist(decklist)
        mainboard = [c for c in result if c['section'] == 'mainboard']
        sideboard = [c for c in result if c['section'] == 'sideboard']
        
        assert len(mainboard) == mainboard_count
        assert len(sideboard) == sideboard_count
    
    def test_parse_decklist_all_mainboard(self):
        """Test parsing decklist with no sideboard section"""
        decklist = "4 Lightning Bolt\n2 Mountain\n1 Island"
        result = parse_decklist(decklist)
        
        assert all(card['section'] == 'mainboard' for card in result)
    
    def test_parse_decklist_all_sideboard(self):
        """Test parsing decklist that starts with sideboard"""
        decklist = "Sideboard\n2 Counterspell\n1 Negate"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert all(card['section'] == 'sideboard' for card in result)


class TestParseDecklistEdgeCases:
    """Tests for handling edge cases (empty decklists, malformed entries, special characters)"""
    
    @pytest.mark.parametrize("decklist", [
        "",
        "   \n\n  \t  "
    ])
    def test_parse_empty_and_whitespace_decklists(self, decklist):
        """Test parsing empty or whitespace-only decklists"""
        result = parse_decklist(decklist)
        assert result == []
    
    def test_parse_decklist_with_empty_lines(self):
        """Test parsing decklist with empty lines"""
        decklist = "4 Lightning Bolt\n\n\n2 Mountain\n\n1 Island"
        result = parse_decklist(decklist)
        
        assert len(result) == 3
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
        assert result[2]['card_name'] == 'Island'
    
    def test_parse_decklist_with_comments(self):
        """Test parsing decklist with comment lines"""
        decklist = "4 Lightning Bolt\n// This is a comment\n2 Mountain\n# Another comment"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
    
    def test_parse_decklist_with_malformed_entries(self):
        """Test parsing decklist with malformed entries"""
        decklist = "4 Lightning Bolt\nInvalid Entry\n2 Mountain\nNo Quantity Here"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'
    
    def test_parse_decklist_with_special_characters_in_name(self):
        """Test parsing decklist with special characters in card names"""
        decklist = "4 Jace, the Mind Sculptor\n2 Æther Vial"
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Jace, the Mind Sculptor'
        assert result[1]['card_name'] == 'Æther Vial'
    
    @pytest.mark.parametrize("decklist,expected_count", [
        ("4 Lightning Bolt\n0 Mountain\n2 Island", 2),  # Skip zero
        ("4 Lightning Bolt\n-1 Mountain\n2 Island", 2),  # Skip negative
        ("4 Lightning Bolt\n4\n2 Mountain", 2),  # Skip quantity-only
        ("4 Lightning Bolt\nMountain\n2 Island", 2),  # Skip card-only
    ])
    def test_parse_decklist_with_invalid_quantities(self, decklist, expected_count):
        """Test parsing decklists with invalid quantities"""
        result = parse_decklist(decklist)
        assert len(result) == expected_count
    
    def test_parse_decklist_with_very_large_quantity(self):
        """Test parsing decklist with very large quantity"""
        decklist = "999 Lightning Bolt"
        result = parse_decklist(decklist)
        
        assert len(result) == 1
        assert result[0]['quantity'] == 999
        assert result[0]['card_name'] == 'Lightning Bolt'
    
    def test_parse_decklist_with_trailing_whitespace(self):
        """Test parsing decklist with trailing whitespace on lines"""
        decklist = "4 Lightning Bolt  \n  2 Mountain  "
        result = parse_decklist(decklist)
        
        assert len(result) == 2
        assert result[0]['card_name'] == 'Lightning Bolt'
        assert result[1]['card_name'] == 'Mountain'


class TestInsertTournament:
    """Tests for insert_tournament method"""
    
    def test_insert_tournament_executes_insert(self, mock_pipeline):
        """Test that tournament is inserted with correct data"""
        pipeline, _ = mock_pipeline
        
        tournament = create_mock_tournament(
            TID='test123',
            tournamentName='Test Tournament',
            format='Standard',
            startDate=1234567890
        )
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert 'test123' in call_args[1]
        assert 'Test Tournament' in call_args[1]
        mock_cursor.close.assert_called_once()
    
    def test_insert_tournament_handles_missing_event_data(self, mock_pipeline):
        """Test that missing event data is handled gracefully"""
        pipeline, _ = mock_pipeline
        
        tournament = create_mock_tournament(TID='test123', tournamentName='Test Tournament')
        del tournament['eventData']
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.execute.assert_called_once()
    
    def test_insert_tournament_closes_cursor_on_error(self, mock_pipeline):
        """Test that cursor is closed even on error"""
        pipeline, _ = mock_pipeline
        
        tournament = create_mock_tournament(TID='test123')
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_cursor.execute.side_effect = Exception("DB Error")
        mock_conn.cursor.return_value = mock_cursor
        
        with pytest.raises(Exception):
            pipeline.insert_tournament(tournament, mock_conn)
        
        mock_cursor.close.assert_called_once()


class TestInsertPlayers:
    """Tests for insert_players method"""
    
    def test_insert_players_executes_batch_insert(self, mock_pipeline):
        """Test that players are inserted in batch"""
        pipeline, _ = mock_pipeline
        
        players = [
            create_mock_player(
                id='player1',
                name='Alice',
                wins=3,
                winsSwiss=3,
                winRate=0.75,
                losses=1,
                points=9,
                standing=1
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_players('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args
            assert mock_cursor == call_args[0][0]
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0][0] == 'player1'
    
    def test_insert_players_handles_empty_list(self, mock_pipeline):
        """Test that empty player list is handled gracefully"""
        pipeline, _ = mock_pipeline
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_players('test123', [], mock_conn)
        
        # Should not create cursor for empty list
        mock_conn.cursor.assert_not_called()
    
    def test_insert_players_handles_missing_fields(self, mock_pipeline):
        """Test that missing player fields default correctly"""
        pipeline, _ = mock_pipeline
        
        players = [{'id': 'player1'}]  # Minimal player data
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_players('test123', players, mock_conn)
            
            call_args = mock_execute_batch.call_args[0][2]
            player_data = call_args[0]
            assert player_data[0] == 'player1'
            assert player_data[2] == ''  # name defaults to empty string
            assert player_data[3] == 0   # wins defaults to 0


class TestInsertDecklists:
    """Tests for insert_decklists method"""
    
    def test_insert_decklists_executes_batch_insert(self, mock_pipeline):
        """Test that decklists are inserted in batch"""
        pipeline, _ = mock_pipeline
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt\n4 Counterspell'},
            {'id': 'player2', 'decklist': '4 Llanowar Elves'}
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            mock_execute_batch.assert_called_once()
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 2
            assert call_args[0][0] == 'player1'
    
    def test_insert_decklists_skips_players_without_decklist(self, mock_pipeline):
        """Test that players without decklists are skipped"""
        pipeline, _ = mock_pipeline
        
        players = [
            {'id': 'player1', 'decklist': '4 Lightning Bolt'},
            {'id': 'player2'}  # No decklist
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            pipeline.insert_decklists('test123', players, mock_conn)
            
            call_args = mock_execute_batch.call_args[0][2]
            assert len(call_args) == 1
    
    def test_insert_decklists_handles_empty_list(self, mock_pipeline):
        """Test that empty player list is handled gracefully"""
        pipeline, _ = mock_pipeline
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_decklists('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()


class TestInsertMatchRounds:
    """Tests for insert_match_rounds method"""
    
    def test_insert_match_rounds_handles_numeric_rounds(self, mock_pipeline):
        """Test that numeric round numbers are handled correctly"""
        pipeline, _ = mock_pipeline
        
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': 'p1'}, {'id': 'p2'}])
                ]
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
    
    def test_insert_match_rounds_handles_string_rounds(self, mock_pipeline):
        """Test that string round names are converted to numbers"""
        pipeline, _ = mock_pipeline
        
        rounds_data = [
            create_mock_round(
                round='Top 8',
                tables=[
                    create_mock_table(table=1, players=[{'id': 'p1'}, {'id': 'p2'}])
                ]
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Check that Top 8 was converted to 1000
                round_data_call = mock_execute_batch.call_args_list[0][0][2]
                assert round_data_call[0][0] == 1000
    
    def test_insert_match_rounds_filters_invalid_matches(self, mock_pipeline):
        """Test that invalid matches are filtered out"""
        pipeline, _ = mock_pipeline
        
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': 'p1'}, {'id': 'p2'}]),
                    create_mock_table(table=2, players=[{'id': 'p3'}, {'id': 'p4'}, {'id': 'p5'}])  # 3 players
                ]
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('p1',), ('p2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', side_effect=[True, False]):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Should have 2 execute_batch calls (rounds and matches)
                matches_call = mock_execute_batch.call_args_list[1][0][2]
                assert len(matches_call) == 1  # Only 1 valid match
    
    def test_insert_match_rounds_handles_empty_list(self, mock_pipeline):
        """Test that empty rounds list is handled gracefully"""
        pipeline, _ = mock_pipeline
        
        mock_conn = Mock()
        
        # Should not raise error
        pipeline.insert_match_rounds('test123', [], mock_conn)
        
        mock_conn.cursor.assert_not_called()
    
    def test_insert_match_rounds_skips_matches_with_missing_players(self, mock_pipeline):
        """Test that matches with players not in database are skipped to avoid FK violations"""
        pipeline, _ = mock_pipeline
        
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': 'player1'}, {'id': 'player2'}]),
                    create_mock_table(table=2, players=[{'id': 'missing_player1'}, {'id': 'player2'}]),
                    create_mock_table(table=3, players=[{'id': 'player1'}, {'id': 'missing_player2'}])
                ]
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock existing players query - only player1 and player2 exist
        mock_cursor.fetchall.return_value = [('player1',), ('player2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch') as mock_execute_batch:
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Should only insert 1 match (table 1), skipping tables 2 and 3
                assert mock_execute_batch.call_count == 2  # One for rounds, one for matches
                matches_call = mock_execute_batch.call_args_list[1]
                match_data = matches_call[0][2]  # Third argument is the data
                assert len(match_data) == 1, "Should only insert 1 match with valid players"
                assert match_data[0][3] == 'player1'  # player1_id
                assert match_data[0][4] == 'player2'  # player2_id
    
    def test_insert_match_rounds_validates_player_existence(self, mock_pipeline):
        """Test that insert_match_rounds queries existing players before inserting matches"""
        pipeline, _ = mock_pipeline
        
        rounds_data = [
            create_mock_round(
                round=1,
                tables=[
                    create_mock_table(table=1, players=[{'id': 'player1'}, {'id': 'player2'}])
                ]
            )
        ]
        
        mock_conn, mock_cursor = Mock(), Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('player1',), ('player2',)]
        
        with patch('src.etl.etl_pipeline.execute_batch'):
            with patch('src.etl.etl_pipeline.is_valid_match', return_value=True):
                pipeline.insert_match_rounds('test123', rounds_data, mock_conn)
                
                # Verify that we queried for existing players
                execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
                player_query_found = any(
                    'SELECT player_id FROM players' in str(call) 
                    for call in execute_calls
                )
                assert player_query_found, "Should query for existing players before inserting matches"

