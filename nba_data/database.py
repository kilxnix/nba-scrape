# First, create nba_data/database.py
# nba_data/database.py
import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

class DatabasePool:
    _pool = None
    
    @classmethod
    def initialize(cls, min_conn=1, max_conn=10):
        if cls._pool is None:
            try:
                cls._pool = pool.SimpleConnectionPool(
                    minconn=min_conn,
                    maxconn=max_conn,
                    dbname=os.getenv('dbname'),
                    user=os.getenv('user'),
                    password=os.getenv('password'),
                    host=os.getenv('host'),
                    port=os.getenv('port'),
                    sslmode='require'
                )
                print("Connection pool initialized successfully")
            except Exception as e:
                print(f"Error initializing connection pool: {e}")
                raise
    
    @classmethod
    def get_connection(cls):
        if cls._pool is None:
            cls.initialize()
        return cls._pool.getconn()
    
    @classmethod
    def return_connection(cls, conn):
        cls._pool.putconn(conn)
    
    @classmethod
    def close_all(cls):
        if cls._pool is not None:
            cls._pool.closeall()

@contextmanager
def db_transaction():
    conn = None
    try:
        conn = DatabasePool.get_connection()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            DatabasePool.return_connection(conn)