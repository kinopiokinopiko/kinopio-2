import sqlite3
from contextlib import contextmanager
from config import get_config
from utils import logger
import time

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as pg_pool
    from psycopg2 import extensions
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    logger.warning("⚠️ psycopg2 not available, using SQLite")

class DatabaseManager:
    """データベース接続を管理"""
    
    def __init__(self, config=None):
        self.config = config or get_config()
        self.pool = None
        self.use_postgres = self.config.USE_POSTGRES and POSTGRES_AVAILABLE
        
        logger.info(f"🔧 DatabaseManager initializing...")
        logger.info(f"📊 USE_POSTGRES: {self.use_postgres}")
        
        if self.use_postgres:
            self._init_pool()
    
    def _init_pool(self):
        """コネクションプール初期化"""
        if self.use_postgres and self.config.DATABASE_URL:
            try:
                logger.info("🔌 Creating PostgreSQL connection pool...")
                self.pool = pg_pool.SimpleConnectionPool(
                    1, 20, self.config.DATABASE_URL, connect_timeout=10
                )
                logger.info("✅ PostgreSQL connection pool initialized")
            except Exception as e:
                logger.error(f"❌ Failed to create connection pool: {e}", exc_info=True)
                self.use_postgres = False
                logger.info("⚠️ Falling back to SQLite")
    
    def _get_connection_with_retry(self, max_retries=3):
        """再接続処理付きでコネクションを取得"""
        last_error = None
        for attempt in range(max_retries):
            try:
                if not self.pool: raise RuntimeError("Database pool not initialized")
                conn = self.pool.getconn()
                if conn.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE:
                    conn.rollback()
                return conn
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                logger.warning(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
                    try:
                        if self.pool: self.pool.closeall()
                        self._init_pool()
                    except Exception: pass
            except Exception as e:
                logger.error(f"❌ Unexpected error: {e}")
                raise
        raise RuntimeError(f"Failed to get DB connection: {last_error}")
    
    @contextmanager
    def get_db(self):
        """データベース接続を取得"""
        if self.use_postgres:
            conn = None
            try:
                conn = self._get_connection_with_retry()
                
                # RealDictCursorラッパー
                class DictConnection:
                    def __init__(self, real_conn): self._conn = real_conn
                    def cursor(self, *args, **kwargs): return self._conn.cursor(cursor_factory=RealDictCursor)
                    def commit(self): return self._conn.commit()
                    def rollback(self): return self._conn.rollback()
                    def close(self): pass # プールに戻すため閉じない
                
                yield DictConnection(conn)
            except Exception as e:
                if conn: 
                    try: conn.rollback()
                    except: pass
                logger.error(f"❌ Database error: {e}", exc_info=True)
                raise
            finally:
                if conn and self.pool:
                    try: self.pool.putconn(conn)
                    except: pass
        else:
            conn = sqlite3.connect('portfolio.db', timeout=30.0)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ SQLite error: {e}", exc_info=True)
                raise
            finally:
                conn.close()
    
    def init_database(self):
        """データベーススキーマを初期化"""
        logger.info("📊 Initializing database schema...")
        with self.get_db() as conn:
            c = conn.cursor()
            if self.use_postgres:
                self._init_postgres(c, conn)
            else:
                self._init_sqlite(c, conn)
            conn.commit()
            logger.info("✅ Database schema initialized successfully")

    def _init_postgres(self, cursor, conn):
        """PostgreSQL テーブル作成"""
        logger.info("✅ Creating PostgreSQL tables...")
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            asset_type VARCHAR(50) NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            name VARCHAR(255),
            quantity DOUBLE PRECISION NOT NULL,
            price DOUBLE PRECISION DEFAULT 0,
            avg_cost DOUBLE PRECISION DEFAULT 0,
            display_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS asset_history (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            record_date DATE NOT NULL,
            jp_stock_value DOUBLE PRECISION DEFAULT 0,
            us_stock_value DOUBLE PRECISION DEFAULT 0,
            cash_value DOUBLE PRECISION DEFAULT 0,
            gold_value DOUBLE PRECISION DEFAULT 0,
            crypto_value DOUBLE PRECISION DEFAULT 0,
            investment_trust_value DOUBLE PRECISION DEFAULT 0,
            insurance_value DOUBLE PRECISION DEFAULT 0,
            total_value DOUBLE PRECISION DEFAULT 0,
            prev_jp_stock_value DOUBLE PRECISION DEFAULT 0,
            prev_us_stock_value DOUBLE PRECISION DEFAULT 0,
            prev_cash_value DOUBLE PRECISION DEFAULT 0,
            prev_gold_value DOUBLE PRECISION DEFAULT 0,
            prev_crypto_value DOUBLE PRECISION DEFAULT 0,
            prev_investment_trust_value DOUBLE PRECISION DEFAULT 0,
            prev_insurance_value DOUBLE PRECISION DEFAULT 0,
            prev_total_value DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
            UNIQUE(user_id, record_date)
        )''')
        
        # カラム追加マイグレーション
        self._migrate_column_pg(cursor, 'asset_history', 'prev_jp_stock_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_us_stock_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_cash_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_gold_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_crypto_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_investment_trust_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_insurance_value', 'DOUBLE PRECISION DEFAULT 0')
        self._migrate_column_pg(cursor, 'asset_history', 'prev_total_value', 'DOUBLE PRECISION DEFAULT 0')
        
        # ✅ 並び順カラムの追加
        self._migrate_column_pg(cursor, 'assets', 'display_order', 'INTEGER DEFAULT 0')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_type ON assets(user_id, asset_type)')
    
    def _migrate_column_pg(self, cursor, table, column, type_def):
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{column}'")
        if not cursor.fetchone():
            logger.info(f"🔄 Migrating: Adding {column} to {table}")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}")

    def _init_sqlite(self, cursor, conn):
        """SQLite テーブル作成"""
        logger.info("✅ Creating SQLite tables...")
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            quantity REAL NOT NULL,
            price REAL DEFAULT 0,
            avg_cost REAL DEFAULT 0,
            display_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        )''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS asset_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            record_date DATE NOT NULL,
            jp_stock_value REAL DEFAULT 0,
            us_stock_value REAL DEFAULT 0,
            cash_value REAL DEFAULT 0,
            gold_value REAL DEFAULT 0,
            crypto_value REAL DEFAULT 0,
            investment_trust_value REAL DEFAULT 0,
            insurance_value REAL DEFAULT 0,
            total_value REAL DEFAULT 0,
            prev_jp_stock_value REAL DEFAULT 0,
            prev_us_stock_value REAL DEFAULT 0,
            prev_cash_value REAL DEFAULT 0,
            prev_gold_value REAL DEFAULT 0,
            prev_crypto_value REAL DEFAULT 0,
            prev_investment_trust_value REAL DEFAULT 0,
            prev_insurance_value REAL DEFAULT 0,
            prev_total_value REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
            UNIQUE(user_id, record_date)
        )''')
        
        # カラム追加マイグレーション
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_jp_stock_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_us_stock_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_cash_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_gold_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_crypto_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_investment_trust_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_insurance_value', 'REAL DEFAULT 0')
        self._migrate_column_sqlite(cursor, 'asset_history', 'prev_total_value', 'REAL DEFAULT 0')
        
        # ✅ 並び順カラムの追加
        self._migrate_column_sqlite(cursor, 'assets', 'display_order', 'INTEGER DEFAULT 0')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_type ON assets(user_id, asset_type)')

    def _migrate_column_sqlite(self, cursor, table, column, type_def):
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row['name'] for row in cursor.fetchall()]
        if column not in columns:
            logger.info(f"🔄 Migrating: Adding {column} to {table}")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}")

db_manager = DatabaseManager()
