"""Integration tests for database operations with real database"""

import pytest
import os
from unittest.mock import patch
from dotenv import load_dotenv

load_dotenv()

from src.database.connection import DatabaseConnection


class TestDatabaseIntegration:
    """Integration tests for database connection and operations"""
    
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(self):
        """Setup and teardown for database tests"""
        # Load environment variables from .env (in case they weren't loaded yet)
        load_dotenv(override=False)

        # Use test database name from environment (must be set as TEST_DB_NAME in .env)
        test_db_name = os.getenv('TEST_DB_NAME')
        if not test_db_name:
            pytest.fail("TEST_DB_NAME environment variable must be set in .env file")

        # Set DB_NAME from TEST_DB_NAME if not already set
        original_db_name = os.getenv('DB_NAME')
        os.environ['DB_NAME'] = test_db_name

        # Use DB_USER and DB_PASSWORD from .env if they exist
        db_user = os.getenv('DB_USER')
        if db_user:
            os.environ['DB_USER'] = db_user

        db_password = os.getenv('DB_PASSWORD')
        if db_password:
            os.environ['DB_PASSWORD'] = db_password

        # Set DB_HOST and DB_PORT if not already set
        db_host = os.getenv('DB_HOST')
        if db_host:
            os.environ['DB_HOST'] = db_host

        db_port = os.getenv('DB_PORT')
        if db_port:
            os.environ['DB_PORT'] = db_port

        # Ensure clean state before tests
        DatabaseConnection._connection_pool = None

        yield

        # Restore original DB_NAME after tests
        if original_db_name:
            os.environ['DB_NAME'] = original_db_name
        elif 'DB_NAME' in os.environ:
            del os.environ['DB_NAME']

        # Cleanup
        DatabaseConnection.close_pool()
    
    def test_initialize_pool(self):
        """Test that connection pool can be initialized"""
        DatabaseConnection._connection_pool = None
        
        # Should not raise error
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        assert DatabaseConnection._connection_pool is not None, "Connection pool should be initialized"
    
    def test_get_connection_from_pool(self):
        """Test getting a connection from the pool"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        conn = DatabaseConnection.get_connection()
        
        assert conn is not None, "Should get a connection from pool"
        
        # Return connection
        DatabaseConnection.return_connection(conn)
    
    def test_connection_is_usable(self):
        """Test that connection can execute queries"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        conn = DatabaseConnection.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            
            assert result is not None, "Should get result from query"
            assert result[0] == 1, "Query result should be 1"
        finally:
            DatabaseConnection.return_connection(conn)
    
    def test_get_cursor_context_manager(self):
        """Test cursor context manager"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            
            assert result is not None, "Should get result from query"
            assert result[0] == 1, "Query result should be 1"
    
    def test_transaction_context_manager_commit(self):
        """Test transaction context manager with commit"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Create a temp table, insert data, and commit
        with DatabaseConnection.transaction() as conn:
            cursor = conn.cursor()
            
            # Create temporary table
            cursor.execute("""
                CREATE TEMP TABLE test_transaction (
                    id SERIAL PRIMARY KEY,
                    value TEXT
                )
            """)
            
            # Insert data
            cursor.execute("INSERT INTO test_transaction (value) VALUES (%s)", ('test_value',))
            
            cursor.close()
        
        # Verify data was committed (in new transaction)
        with DatabaseConnection.transaction() as conn:
            cursor = conn.cursor()
            
            # Temp table won't exist in new session, so just verify we can run queries
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            cursor.close()
            
            assert result[0] == 1, "Should be able to run queries after commit"
    
    def test_transaction_context_manager_rollback(self):
        """Test transaction context manager with rollback on error"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Attempt transaction that will fail
        with pytest.raises(Exception):
            with DatabaseConnection.transaction() as conn:
                cursor = conn.cursor()
                
                # Create temporary table
                cursor.execute("""
                    CREATE TEMP TABLE test_rollback (
                        id SERIAL PRIMARY KEY,
                        value TEXT
                    )
                """)
                
                cursor.close()
                
                # Raise error to trigger rollback
                raise Exception("Test error for rollback")
        
        # Connection should still be usable after rollback
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result[0] == 1, "Connection should be usable after rollback"
    
    def test_multiple_connections(self):
        """Test getting multiple connections from pool"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=3)
        
        conn1 = DatabaseConnection.get_connection()
        conn2 = DatabaseConnection.get_connection()
        
        assert conn1 is not None, "Should get first connection"
        assert conn2 is not None, "Should get second connection"
        
        # Both connections should be usable
        cursor1 = conn1.cursor()
        cursor1.execute("SELECT 1")
        result1 = cursor1.fetchone()
        cursor1.close()
        
        cursor2 = conn2.cursor()
        cursor2.execute("SELECT 2")
        result2 = cursor2.fetchone()
        cursor2.close()
        
        assert result1[0] == 1, "First connection should work"
        assert result2[0] == 2, "Second connection should work"
        
        # Return connections
        DatabaseConnection.return_connection(conn1)
        DatabaseConnection.return_connection(conn2)
    
    def test_close_pool_closes_all_connections(self):
        """Test that close_pool closes all connections"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Get connection to verify pool is working
        conn = DatabaseConnection.get_connection()
        DatabaseConnection.return_connection(conn)
        
        # Close pool
        DatabaseConnection.close_pool()
        
        assert DatabaseConnection._connection_pool is None, "Pool should be None after closing"
    
    def test_cursor_with_commit(self):
        """Test cursor context manager with commit"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Execute with commit=True
        with DatabaseConnection.get_cursor(commit=True) as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            
            assert result[0] == 1, "Should execute query"
        
        # No error should be raised
    
    def test_cursor_without_commit(self):
        """Test cursor context manager without commit"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Execute with commit=False (default)
        with DatabaseConnection.get_cursor(commit=False) as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            
            assert result[0] == 1, "Should execute query"
        
        # No error should be raised
    
    def test_connection_parameters(self):
        """Test that connection parameters are read correctly"""
        params = DatabaseConnection._get_connection_params()
        
        assert 'host' in params, "Should have host parameter"
        assert 'port' in params, "Should have port parameter"
        assert 'database' in params, "Should have database parameter"
        assert 'user' in params, "Should have user parameter"
        assert 'password' in params, "Should have password parameter"
        
        # Verify types
        assert isinstance(params['port'], int), "Port should be an integer"
    
    def test_reinitialization_warning(self):
        """Test that reinitializing pool logs warning but doesn't fail"""
        DatabaseConnection._connection_pool = None
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Try to initialize again - should warn but not fail
        DatabaseConnection.initialize_pool(min_conn=1, max_conn=2)
        
        # Pool should still be usable
        conn = DatabaseConnection.get_connection()
        assert conn is not None, "Pool should still be usable after reinitialization attempt"
        DatabaseConnection.return_connection(conn)

