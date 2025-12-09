"""PostgreSQL database connection manager with connection pooling"""

import os
import logging
from contextlib import contextmanager
from typing import Optional, Generator
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extensions import connection, cursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL database connections with connection pooling"""
    
    _connection_pool: Optional[pool.ThreadedConnectionPool] = None
    _current_database: Optional[str] = None
    
    @classmethod
    def _get_connection_params(cls, database: Optional[str] = None) -> dict:
        """
        Get database connection parameters from environment variables
        
        Args:
            database: Optional database name. If None, uses DB_NAME from environment.
                     Use 'postgres' to connect to default PostgreSQL database.
        """
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': database or os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
        }
    
    @classmethod
    def initialize_pool(cls, database: Optional[str] = None, min_conn: int = 1, max_conn: int = 10) -> None:
        """
        Initialize the connection pool
        
        Args:
            database: Optional database name. If None, uses DB_NAME from environment.
                     If specified and different from current pool, closes existing pool and creates new one.
            min_conn: Minimum number of connections in pool
            max_conn: Maximum number of connections in pool
        """
        # Determine target database
        target_database = database or os.getenv('DB_NAME')
        
        # If pool exists and database matches, no need to reinitialize
        if cls._connection_pool is not None:
            if cls._current_database == target_database:
                logger.debug(f"Connection pool already initialized for database '{target_database}'")
                return
            else:
                logger.info(f"Reinitializing connection pool: '{cls._current_database}' -> '{target_database}'")
                cls.close_pool()
        
        params = cls._get_connection_params(database=database)
        
        # Validate required parameters
        required_params = ['database', 'user', 'password']
        missing_params = [p for p in required_params if not params.get(p)]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")
        
        try:
            cls._connection_pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                **params
            )
            cls._current_database = target_database
            logger.info(f"Database connection pool initialized for '{target_database}' ({min_conn}-{max_conn} connections)")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @classmethod
    def get_connection(cls) -> connection:
        """Get a connection from the pool"""
        if cls._connection_pool is None:
            logger.debug("Connection pool not initialized, initializing now")
            cls.initialize_pool()
        
        try:
            conn = cls._connection_pool.getconn()
            if conn is None:
                logger.error("Failed to get connection from pool: connection is None")
                raise RuntimeError("Failed to get connection from pool")
            logger.debug("Successfully retrieved connection from pool")
            return conn
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise
    
    @classmethod
    def return_connection(cls, conn: connection) -> None:
        """Return a connection to the pool"""
        if cls._connection_pool is None:
            logger.warning("Connection pool not initialized")
            return
        
        try:
            cls._connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to return connection to pool: {e}")
    
    @classmethod
    @contextmanager
    def get_cursor(cls, commit: bool = False) -> Generator[cursor, None, None]:
        """
        Context manager for database cursor
        
        Args:
            commit: If True, commit transaction on exit. If False, rollback on error.
        
        Yields:
            Database cursor
        """
        conn = None
        try:
            conn = cls.get_connection()
            cur = conn.cursor()
            logger.debug("Database cursor created")
            yield cur
            if commit:
                logger.debug("Committing transaction")
                conn.commit()
            else:
                logger.debug("Rolling back transaction (no commit requested)")
                conn.rollback()
        except Exception as e:
            if conn:
                logger.debug("Rolling back transaction due to error")
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                cur.close()
                logger.debug("Cursor closed, returning connection to pool")
                cls.return_connection(conn)
    
    @classmethod
    @contextmanager
    def transaction(cls) -> Generator[connection, None, None]:
        """
        Context manager for database transaction
        
        Yields:
            Database connection (commits on success, rolls back on error)
        """
        conn = None
        try:
            conn = cls.get_connection()
            logger.debug("Transaction started")
            yield conn
            logger.debug("Committing transaction")
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            if conn:
                logger.debug("Rolling back transaction due to error")
                conn.rollback()
            logger.error(f"Transaction failed: {e}")
            raise
        finally:
            if conn:
                logger.debug("Returning connection to pool")
                cls.return_connection(conn)
    
    @classmethod
    def close_pool(cls) -> None:
        """Close all connections in the pool"""
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            cls._connection_pool = None
            cls._current_database = None
            logger.info("Connection pool closed")
    
    @classmethod
    def execute_schema_file(cls, schema_file_path: str, database: Optional[str] = None) -> None:
        """
        Execute a SQL schema file
        
        Args:
            schema_file_path: Path to the SQL schema file
            database: Optional database name. If None, uses DB_NAME from environment.
        """
        with open(schema_file_path, 'r') as f:
            schema_sql = f.read()
        
        # If database is specified, create a direct connection to it
        # Otherwise, use the connection pool
        if database:
            params = cls._get_connection_params(database=database)
            required_params = ['user', 'password']
            missing_params = [p for p in required_params if not params.get(p)]
            if missing_params:
                raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")
            
            conn = None
            try:
                conn = psycopg2.connect(**params)
                cur = conn.cursor()
                cur.execute(schema_sql)
                conn.commit()
                cur.close()
                logger.info(f"Schema file executed: {schema_file_path} on database: {database}")
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Failed to execute schema file on database '{database}': {e}")
                raise
            finally:
                if conn:
                    conn.close()
        else:
            # Use connection pool for default database
            with cls.transaction() as conn:
                cur = conn.cursor()
                cur.execute(schema_sql)
                cur.close()
            
            logger.info(f"Schema file executed: {schema_file_path}")
    
    @classmethod
    def database_exists(cls, database_name: str) -> bool:
        """
        Check if a database exists
        
        Args:
            database_name: Name of the database to check
            
        Returns:
            True if database exists, False otherwise
        """
        params = cls._get_connection_params(database='postgres')
        
        # Validate required parameters
        required_params = ['user', 'password']
        missing_params = [p for p in required_params if not params.get(p)]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")
        
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cur = conn.cursor()
            
            # Check if database exists
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (database_name,)
            )
            exists = cur.fetchone() is not None
            
            cur.close()
            conn.close()
            
            return exists
        except Exception as e:
            logger.error(f"Failed to check if database exists: {e}")
            raise
    
    @classmethod
    def create_database(cls, database_name: str) -> None:
        """
        Create a database if it doesn't exist
        
        Args:
            database_name: Name of the database to create
        """
        if cls.database_exists(database_name):
            logger.info(f"Database '{database_name}' already exists")
            return
        
        params = cls._get_connection_params(database='postgres')
        
        # Validate required parameters
        required_params = ['user', 'password']
        missing_params = [p for p in required_params if not params.get(p)]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")
        
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cur = conn.cursor()
            
            # Create database
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(database_name)
            ))
            
            cur.close()
            conn.close()
            
            logger.info(f"Database '{database_name}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create database '{database_name}': {e}")
            raise
    
    @classmethod
    def drop_database(cls, database_name: str) -> None:
        """
        Drop a database if it exists
        
        Args:
            database_name: Name of the database to drop
        """
        if not cls.database_exists(database_name):
            logger.info(f"Database '{database_name}' does not exist, nothing to drop")
            return
        
        params = cls._get_connection_params(database='postgres')
        
        # Validate required parameters
        required_params = ['user', 'password']
        missing_params = [p for p in required_params if not params.get(p)]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {', '.join(missing_params)}")
        
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cur = conn.cursor()
            
            # Terminate existing connections to the database
            cur.execute(sql.SQL("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                AND pid <> pg_backend_pid()
            """), [database_name])
            
            # Drop database
            cur.execute(sql.SQL("DROP DATABASE {}").format(
                sql.Identifier(database_name)
            ))
            
            cur.close()
            conn.close()
            logger.info(f"Database '{database_name}' dropped successfully")
        except Exception as e:
            logger.error(f"Failed to drop database '{database_name}': {e}")
            raise
    
    @classmethod
    def ensure_database_exists(cls, database_name: str) -> None:
        """
        Ensure a database exists, creating it if necessary
        
        Args:
            database_name: Name of the database to ensure exists
        """
        if not cls.database_exists(database_name):
            cls.create_database(database_name)
        else:
            logger.info(f"Database '{database_name}' already exists")
    
    @classmethod
    def initialize_database(cls, database_name: str, schema_file_path: str) -> None:
        """
        Initialize a database: ensure it exists and execute the schema file
        
        Args:
            database_name: Name of the database to initialize
            schema_file_path: Path to the SQL schema file
        """
        logger.info(f"Initializing database '{database_name}'...")
        cls.ensure_database_exists(database_name)
        cls.execute_schema_file(schema_file_path, database=database_name)
        logger.info(f"Database '{database_name}' initialized successfully")

