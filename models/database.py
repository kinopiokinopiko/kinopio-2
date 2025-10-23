import sqlite3
from contextlib import contextmanager
from config import get_config
from utils import logger

# ================================================================================
# 📊 データベース管理
# ================================================================================

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as pg_pool
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

class DatabaseManager:
    """データベース接続を管理"""
    
    def __init__(self, config=None):
        self.config = config or get_config()
        self.pool = None
        self.use_postgres = self.config.USE_POSTGRES and POSTGRES_AVAILABLE
        self._init_pool()
    
    def _init_pool(self):
        """コネクションプール初期化"""
        if self.use_postgres and self.config.DATABASE_URL:
            try:
                self.pool = pg_pool.SimpleConnectionPool(2, 10, self.config.DATABASE_URL)
                logger.info("✅ Database connection pool initialized (PostgreSQL)")
            except Exception as e:
                logger.error(f"❌ Failed to create connection pool: {e}")
                self.use_postgres = False
    
    @contextmanager
    def get_db(self):
        """データベース接続を取得"""
        if self.use_postgres:
            if self.pool:
                conn = self.pool.getconn()
                try:
                    conn.set_session(autocommit=False)
                    yield conn
                finally:
                    self.pool.putconn(conn)
            else:
                raise RuntimeError("Database pool not initialized")
        else:
            conn = sqlite3.connect('portfolio.db')
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()
    
    def init_database(self):
        """データベーススキーマを初期化"""
        with self.get_db() as conn:
            c = conn.cursor()
            
            if self.use_postgres:
                self._init_postgres(c, conn)
            else:
                self._init_sqlite(c, conn)
    
    def _init_postgres(self, cursor, conn):
        """PostgreSQL テーブル作成"""
        try:
            # usersテーブル
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # assetsテーブル
            cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                asset_type VARCHAR(50) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                name VARCHAR(255),
                quantity DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION DEFAULT 0,
                avg_cost DOUBLE PRECISION DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )''')
            
            # asset_historyテーブル（前日比カラムを含む）
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
            
            # インデックスを作成
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_assets_user_id 
                            ON assets(user_id)''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_asset_history_user_id 
                            ON asset_history(user_id)''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_asset_history_user_date 
                            ON asset_history(user_id, record_date)''')
            
            conn.commit()
            
            # デモユーザー作成
            cursor.execute("SELECT id FROM users WHERE username = %s", ('demo',))
            if not cursor.fetchone():
                from werkzeug.security import generate_password_hash
                demo_hash = generate_password_hash('demo123')
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                             ('demo', demo_hash))
                conn.commit()
                logger.info("✅ Demo user created")
            
            logger.info("✅ PostgreSQL database initialized successfully")
        
        except Exception as e:
            logger.error(f"❌ Error initializing PostgreSQL database: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _init_sqlite(self, cursor, conn):
        """SQLite テーブル作成"""
        try:
            # usersテーブル - created_at は含めない（既存テーブルに対応）
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )''')
            
            # assetsテーブル
            cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                asset_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                quantity REAL NOT NULL,
                price REAL DEFAULT 0,
                avg_cost REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )''')
            
            # asset_history テーブル作成（前日比カラムを含む）
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
            
            conn.commit()
            logger.info("✅ SQLite database initialized successfully")
            
            # デモユーザー作成
            cursor.execute("SELECT id FROM users WHERE username = ?", ('demo',))
            if not cursor.fetchone():
                from werkzeug.security import generate_password_hash
                demo_hash = generate_password_hash('demo123')
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                             ('demo', demo_hash))
                conn.commit()
                logger.info("✅ Demo user (demo/demo123) created")
        
        except Exception as e:
            logger.error(f"❌ Error initializing SQLite database: {e}", exc_info=True)
            conn.rollback()
            raise

# グローバルデータベースマネージャー
db_manager = DatabaseManager()