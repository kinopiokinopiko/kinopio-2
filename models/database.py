import sqlite3
from contextlib import contextmanager
from config import get_config
from utils import logger

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as pg_pool
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
        logger.info(f"📊 DATABASE_URL: {self.config.DATABASE_URL[:50] if self.config.DATABASE_URL else 'None'}...")
        
        if self.use_postgres:
            self._init_pool()
    
def _init_pool(self):
    """コネクションプール初期化（RealDictCursorをデフォルトに設定）"""
    if self.use_postgres and self.config.DATABASE_URL:
        try:
            logger.info("🔌 Creating PostgreSQL connection pool...")
            # ✅ 修正: プール作成時にcursor_factoryを設定
            self.pool = pg_pool.SimpleConnectionPool(
                1,  # minconn
                10, # maxconn
                self.config.DATABASE_URL,
                cursor_factory=RealDictCursor  # ✅ ここで設定
            )
            logger.info("✅ PostgreSQL connection pool initialized with RealDictCursor")
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}", exc_info=True)
            self.use_postgres = False
            logger.info("⚠️ Falling back to SQLite")
    
    @contextmanager
    def get_db(self):
        """データベース接続を取得"""
        if self.use_postgres:
            if not self.pool:
                raise RuntimeError("Database pool not initialized")
            
            conn = None
            try:
                conn = self.pool.getconn()
                conn.set_session(autocommit=False)
                
                # ✅ RealDictCursor を使用
                original_cursor_factory = conn.cursor_factory
                conn.cursor_factory = RealDictCursor
                
                yield conn
                
                # 元に戻す
                conn.cursor_factory = original_cursor_factory
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"❌ Database error: {e}", exc_info=True)
                raise
            finally:
                if conn:
                    self.pool.putconn(conn)
        else:
            conn = sqlite3.connect('portfolio.db')
            conn.row_factory = sqlite3.Row
            try:
                yield conn
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
    
    def _init_postgres(self, cursor, conn):
        """PostgreSQL テーブル作成"""
        try:
            logger.info("✅ Creating PostgreSQL tables...")
            
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
            
            # asset_historyテーブル
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
            
            # インデックス作成
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_id ON asset_history(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_date ON asset_history(user_id, record_date)')
            
            conn.commit()
            logger.info("✅ PostgreSQL tables created")
            
            # ✅ デモユーザー作成
            from werkzeug.security import generate_password_hash
            
            cursor.execute("SELECT id FROM users WHERE username = %s", ('demo',))
            if not cursor.fetchone():
                demo_hash = generate_password_hash('demo123')
                logger.info(f"🔐 Creating demo user (hash: {demo_hash[:30]}...)")
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                             ('demo', demo_hash))
                conn.commit()
                logger.info("✅ Demo user created: demo/demo123")
            else:
                logger.info("ℹ️ Demo user already exists")
            
            logger.info("✅ PostgreSQL database initialized successfully")
        
        except Exception as e:
            logger.error(f"❌ Error initializing PostgreSQL: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _init_sqlite(self, cursor, conn):
        """SQLite テーブル作成"""
        try:
            logger.info("✅ Creating SQLite tables...")
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
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
            
            conn.commit()
            logger.info("✅ SQLite tables created")
            
            # デモユーザー作成
            from werkzeug.security import generate_password_hash
            
            cursor.execute("SELECT id FROM users WHERE username = ?", ('demo',))
            if not cursor.fetchone():
                demo_hash = generate_password_hash('demo123')
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                             ('demo', demo_hash))
                conn.commit()
                logger.info("✅ Demo user created: demo/demo123")
        
        except Exception as e:
            logger.error(f"❌ Error initializing SQLite: {e}", exc_info=True)
            conn.rollback()
            raise

# グローバルデータベースマネージャー
db_manager = DatabaseManager()

