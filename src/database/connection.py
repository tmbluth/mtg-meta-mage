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
    
    @classmethod
    def _get_connection_params(cls) -> dict:
        """Get database connection parameters from environment variables"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
        }
    
    @classmethod
    def initialize_pool(cls, min_conn: int = 1, max_conn: int = 10) -> None:
        """Initialize the connection pool"""
        if cls._connection_pool is not None:
            logger.warning("Connection pool already initialized")
            return
        
        params = cls._get_connection_params()
        
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
            logger.info(f"Database connection pool initialized ({min_conn}-{max_conn} connections)")
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
            logger.info("Connection pool closed")
    
    @classmethod
    def execute_schema_file(cls, schema_file_path: str) -> None:
        """
        Execute a SQL schema file
        
        Args:
            schema_file_path: Path to the SQL schema file
        """
        with open(schema_file_path, 'r') as f:
            schema_sql = f.read()
        
        with cls.transaction() as conn:
            cur = conn.cursor()
            cur.execute(schema_sql)
            cur.close()
        
        logger.info(f"Schema file executed: {schema_file_path}")

