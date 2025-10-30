import sqlite3
from contextlib import contextmanager
from config import get_config
from utils import logger
import time
import os

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as pg_pool
    from psycopg2 import extensions
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    logger.warning("⚠️ psycopg2 not available")

class DatabaseManager:
    """データベース接続を管理"""
    
    def __init__(self, config=None):
        self.config = config or get_config()
        self.pool = None
        self.use_postgres = self.config.USE_POSTGRES and POSTGRES_AVAILABLE
        
        # ✅ Render環境の検出
        self.is_render = os.environ.get('RENDER') is not None
        
        logger.info(f"🔧 DatabaseManager initializing...")
        logger.info(f"🌐 Environment: {'Render' if self.is_render else 'Local'}")
        logger.info(f"📊 USE_POSTGRES: {self.use_postgres}")
        logger.info(f"📊 DATABASE_URL: {self.config.DATABASE_URL[:50] if self.config.DATABASE_URL else 'None'}...")
        
        # ✅ Render環境でPostgreSQLが使えない場合はエラー
        if self.is_render and not self.use_postgres:
            error_msg = (
                "❌ CRITICAL ERROR: Render environment must use PostgreSQL!\n"
                "DATABASE_URL is not set or psycopg2 is not installed.\n"
                "Please check:\n"
                "1. DATABASE_URL environment variable in Render dashboard\n"
                "2. psycopg2-binary in requirements.txt"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        if self.use_postgres:
            self._init_pool()
    
    def _init_pool(self):
        """コネクションプール初期化"""
        if self.use_postgres and self.config.DATABASE_URL:
            try:
                logger.info("🔌 Creating PostgreSQL connection pool...")
                
                # ✅ Neon用の接続設定
                import urllib.parse
                
                result = urllib.parse.urlparse(self.config.DATABASE_URL)
                
                connection_params = {
                    'user': result.username,
                    'password': result.password,
                    'host': result.hostname,
                    'port': result.port or 5432,
                    'database': result.path[1:],
                    'sslmode': 'require',
                    'connect_timeout': 10,
                    'keepalives': 1,
                    'keepalives_idle': 30,
                    'keepalives_interval': 10,
                    'keepalives_count': 5,
                    'options': '-c statement_timeout=30000'
                }
                
                logger.info(f"📊 Connecting to: {result.hostname}:{result.port or 5432}/{result.path[1:]}")
                
                self.pool = pg_pool.SimpleConnectionPool(
                    1,   # minconn
                    10,  # maxconn
                    **connection_params
                )
                logger.info("✅ PostgreSQL connection pool initialized")
                
                # 接続テスト
                test_conn = self.pool.getconn()
                try:
                    cursor = test_conn.cursor()
                    cursor.execute('SELECT version()')
                    version = cursor.fetchone()[0]
                    logger.info(f"✅ Database version: {version[:100]}...")
                    
                    if 'neon' in version.lower():
                        logger.info("✅ Connected to Neon PostgreSQL!")
                    
                    cursor.close()
                    test_conn.commit()
                finally:
                    self.pool.putconn(test_conn)
                
            except Exception as e:
                logger.error(f"❌ Failed to create connection pool: {e}", exc_info=True)
                
                # ✅ Render環境ではエラーで停止
                if self.is_render:
                    raise RuntimeError(f"Failed to connect to PostgreSQL in Render environment: {e}")
                
                # ローカル環境ではSQLiteにフォールバック
                self.use_postgres = False
                logger.info("⚠️ Falling back to SQLite (local environment only)")
    
    def _test_connection(self, conn):
        """接続が有効かテスト"""
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.close()
            return True
        except Exception:
            return False
    
    def _get_connection_with_retry(self, max_retries=3):
        """再接続処理付きでコネクションを取得"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                if not self.pool:
                    raise RuntimeError("Database pool not initialized")
                
                conn = self.pool.getconn()
                
                if conn.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE:
                    try:
                        conn.rollback()
                    except Exception as e:
                        logger.warning(f"⚠️ Rollback during connection reset: {e}")
                
                if not self._test_connection(conn):
                    logger.warning(f"⚠️ Connection test failed on attempt {attempt + 1}")
                    try:
                        self.pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    raise psycopg2.OperationalError("Connection test failed")
                
                logger.debug(f"✅ Connection acquired on attempt {attempt + 1}")
                return conn
            
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                logger.warning(f"⚠️ Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                
                if attempt < max_retries - 1:
                    sleep_time = 0.5 * (2 ** attempt)
                    logger.info(f"⏳ Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    
                    try:
                        logger.info("🔄 Reinitializing connection pool...")
                        if self.pool:
                            try:
                                self.pool.closeall()
                            except Exception as close_error:
                                logger.warning(f"⚠️ Error closing pool: {close_error}")
                        self._init_pool()
                    except Exception as reinit_error:
                        logger.error(f"❌ Pool reinitialization failed: {reinit_error}")
            
            except Exception as e:
                last_error = e
                logger.error(f"❌ Unexpected error getting connection: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        
        raise RuntimeError(f"Failed to get database connection after {max_retries} retries: {last_error}")
    
    @contextmanager
    def get_db(self):
        """データベース接続を取得"""
        if self.use_postgres:
            conn = None
            try:
                conn = self._get_connection_with_retry()
                
                cursor = conn.cursor()
                cursor.execute('SET statement_timeout = 30000')
                cursor.close()
                
                class DictConnection:
                    def __init__(self, real_conn, manager):
                        self._conn = real_conn
                        self._manager = manager
                        self._closed = False
                    
                    def cursor(self, *args, **kwargs):
                        if self._closed:
                            raise psycopg2.InterfaceError("Connection already closed")
                        return self._conn.cursor(cursor_factory=RealDictCursor)
                    
                    def commit(self):
                        if not self._closed:
                            try:
                                return self._conn.commit()
                            except Exception as e:
                                logger.error(**

