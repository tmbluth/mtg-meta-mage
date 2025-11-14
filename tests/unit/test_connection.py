"""Unit tests for database connection manager"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from psycopg2 import pool

from src.database.connection import DatabaseConnection


class TestGetConnectionParams:
    """Tests for _get_connection_params method"""
    
    def test_get_connection_params_from_env(self):
        """Test getting connection parameters from environment variables"""
        env_vars = {
            'DB_HOST': 'testhost',
            'DB_PORT': '5433',
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        with patch.dict('os.environ', env_vars):
            params = DatabaseConnection._get_connection_params()
            
            assert params['host'] == 'testhost'
            assert params['port'] == 5433
            assert params['database'] == 'testdb'
            assert params['user'] == 'testuser'
            assert params['password'] == 'testpass'
    
    def test_get_connection_params_defaults(self):
        """Test default values for optional parameters"""
        env_vars = {
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        with patch.dict('os.environ', env_vars, clear=True):
            params = DatabaseConnection._get_connection_params()
            
            assert params['host'] == 'localhost'
            assert params['port'] == 5432


class TestInitializePool:
    """Tests for initialize_pool method"""
    
    def setUp(self):
        """Reset the connection pool before each test"""
        DatabaseConnection._connection_pool = None
    
    def tearDown(self):
        """Clean up connection pool after each test"""
        if DatabaseConnection._connection_pool:
            DatabaseConnection._connection_pool = None
    
    def test_initialize_pool_creates_pool(self):
        """Test that initialize_pool creates a ThreadedConnectionPool"""
        DatabaseConnection._connection_pool = None
        env_vars = {
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        with patch.dict('os.environ', env_vars):
            with patch('psycopg2.pool.ThreadedConnectionPool') as mock_pool:
                DatabaseConnection.initialize_pool(min_conn=2, max_conn=5)
                
                mock_pool.assert_called_once()
                call_kwargs = mock_pool.call_args[1]
                assert call_kwargs['database'] == 'testdb'
                assert call_kwargs['user'] == 'testuser'
    
    def test_initialize_pool_warns_if_already_initialized(self):
        """Test that initializing twice logs a warning"""
        env_vars = {
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        with patch.dict('os.environ', env_vars):
            with patch('psycopg2.pool.ThreadedConnectionPool'):
                DatabaseConnection._connection_pool = None
                DatabaseConnection.initialize_pool()
                
                # Try to initialize again
                DatabaseConnection.initialize_pool()
                # Should not raise error, just warn
    
    def test_initialize_pool_raises_on_missing_params(self):
        """Test that missing required parameters raises ValueError"""
        DatabaseConnection._connection_pool = None
        
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="Missing required database parameters"):
                DatabaseConnection.initialize_pool()
    
    def test_initialize_pool_handles_connection_error(self):
        """Test that connection errors are raised"""
        DatabaseConnection._connection_pool = None
        env_vars = {
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        with patch.dict('os.environ', env_vars):
            with patch('psycopg2.pool.ThreadedConnectionPool', side_effect=psycopg2.Error("Connection failed")):
                with pytest.raises(psycopg2.Error):
                    DatabaseConnection.initialize_pool()


class TestGetConnection:
    """Tests for get_connection method"""
    
    def test_get_connection_initializes_pool_if_not_exists(self):
        """Test that get_connection initializes pool if not already done"""
        DatabaseConnection._connection_pool = None
        
        env_vars = {
            'DB_NAME': 'testdb',
            'DB_USER': 'testuser',
            'DB_PASSWORD': 'testpass'
        }
        
        mock_pool = Mock()
        mock_conn = Mock()
        mock_pool.getconn.return_value = mock_conn
        
        with patch.dict('os.environ', env_vars):
            with patch('psycopg2.pool.ThreadedConnectionPool', return_value=mock_pool):
                conn = DatabaseConnection.get_connection()
                assert conn == mock_conn
    
    def test_get_connection_returns_connection_from_pool(self):
        """Test that get_connection returns a connection from the pool"""
        mock_pool = Mock()
        mock_conn = Mock()
        mock_pool.getconn.return_value = mock_conn
        DatabaseConnection._connection_pool = mock_pool
        
        conn = DatabaseConnection.get_connection()
        
        assert conn == mock_conn
        mock_pool.getconn.assert_called_once()
    
    def test_get_connection_raises_on_none_connection(self):
        """Test that None connection raises RuntimeError"""
        mock_pool = Mock()
        mock_pool.getconn.return_value = None
        DatabaseConnection._connection_pool = mock_pool
        
        with pytest.raises(RuntimeError, match="Failed to get connection from pool"):
            DatabaseConnection.get_connection()
    
    def test_get_connection_handles_pool_error(self):
        """Test that pool errors are raised"""
        mock_pool = Mock()
        mock_pool.getconn.side_effect = Exception("Pool error")
        DatabaseConnection._connection_pool = mock_pool
        
        with pytest.raises(Exception, match="Pool error"):
            DatabaseConnection.get_connection()


class TestReturnConnection:
    """Tests for return_connection method"""
    
    def test_return_connection_puts_connection_back(self):
        """Test that return_connection returns connection to pool"""
        mock_pool = Mock()
        mock_conn = Mock()
        DatabaseConnection._connection_pool = mock_pool
        
        DatabaseConnection.return_connection(mock_conn)
        
        mock_pool.putconn.assert_called_once_with(mock_conn)
    
    def test_return_connection_handles_no_pool(self):
        """Test that return_connection handles uninitialized pool gracefully"""
        DatabaseConnection._connection_pool = None
        mock_conn = Mock()
        
        # Should not raise error
        DatabaseConnection.return_connection(mock_conn)
    
    def test_return_connection_handles_error(self):
        """Test that errors in returning connection are logged but not raised"""
        mock_pool = Mock()
        mock_pool.putconn.side_effect = Exception("Return error")
        DatabaseConnection._connection_pool = mock_pool
        mock_conn = Mock()
        
        # Should not raise error
        DatabaseConnection.return_connection(mock_conn)


class TestGetCursor:
    """Tests for get_cursor context manager"""
    
    def test_get_cursor_yields_cursor(self):
        """Test that get_cursor yields a cursor"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.get_cursor() as cur:
                    assert cur == mock_cursor
    
    def test_get_cursor_commits_when_requested(self):
        """Test that cursor commits when commit=True"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.get_cursor(commit=True):
                    pass
                
                mock_conn.commit.assert_called_once()
    
    def test_get_cursor_rolls_back_when_no_commit(self):
        """Test that cursor rolls back when commit=False"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.get_cursor(commit=False):
                    pass
                
                mock_conn.rollback.assert_called()
    
    def test_get_cursor_rolls_back_on_error(self):
        """Test that cursor rolls back on exception"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with pytest.raises(ValueError):
                    with DatabaseConnection.get_cursor(commit=True):
                        raise ValueError("Test error")
                
                mock_conn.rollback.assert_called()
    
    def test_get_cursor_closes_cursor(self):
        """Test that cursor is closed after use"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.get_cursor():
                    pass
                
                mock_cursor.close.assert_called_once()
    
    def test_get_cursor_returns_connection_to_pool(self):
        """Test that connection is returned to pool after use"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection') as mock_return:
                with DatabaseConnection.get_cursor():
                    pass
                
                mock_return.assert_called_once_with(mock_conn)


class TestTransaction:
    """Tests for transaction context manager"""
    
    def test_transaction_yields_connection(self):
        """Test that transaction yields a connection"""
        mock_conn = Mock()
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.transaction() as conn:
                    assert conn == mock_conn
    
    def test_transaction_commits_on_success(self):
        """Test that transaction commits on successful completion"""
        mock_conn = Mock()
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with DatabaseConnection.transaction():
                    pass
                
                mock_conn.commit.assert_called_once()
    
    def test_transaction_rolls_back_on_error(self):
        """Test that transaction rolls back on exception"""
        mock_conn = Mock()
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection'):
                with pytest.raises(ValueError):
                    with DatabaseConnection.transaction():
                        raise ValueError("Test error")
                
                mock_conn.rollback.assert_called()
    
    def test_transaction_returns_connection_to_pool(self):
        """Test that connection is returned to pool after transaction"""
        mock_conn = Mock()
        
        with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
            with patch.object(DatabaseConnection, 'return_connection') as mock_return:
                with DatabaseConnection.transaction():
                    pass
                
                mock_return.assert_called_once_with(mock_conn)


class TestClosePool:
    """Tests for close_pool method"""
    
    def test_close_pool_closes_all_connections(self):
        """Test that close_pool closes all connections in pool"""
        mock_pool = Mock()
        DatabaseConnection._connection_pool = mock_pool
        
        DatabaseConnection.close_pool()
        
        mock_pool.closeall.assert_called_once()
        assert DatabaseConnection._connection_pool is None
    
    def test_close_pool_handles_no_pool(self):
        """Test that close_pool handles uninitialized pool gracefully"""
        DatabaseConnection._connection_pool = None
        
        # Should not raise error
        DatabaseConnection.close_pool()


class TestExecuteSchemaFile:
    """Tests for execute_schema_file method"""
    
    def test_execute_schema_file_reads_and_executes_sql(self):
        """Test that schema file is read and executed"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        schema_sql = "CREATE TABLE test (id INT);"
        
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = schema_sql
            
            with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
                with patch.object(DatabaseConnection, 'return_connection'):
                    DatabaseConnection.execute_schema_file('/path/to/schema.sql')
                    
                    mock_cursor.execute.assert_called_once_with(schema_sql)
                    mock_cursor.close.assert_called_once()
    
    def test_execute_schema_file_commits_transaction(self):
        """Test that schema execution is committed"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = "CREATE TABLE test (id INT);"
            
            with patch.object(DatabaseConnection, 'get_connection', return_value=mock_conn):
                with patch.object(DatabaseConnection, 'return_connection'):
                    DatabaseConnection.execute_schema_file('/path/to/schema.sql')
                    
                    mock_conn.commit.assert_called_once()

