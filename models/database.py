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
        logger.info(f"📊 DATABASE_URL: {self.config.DATABASE_URL[:50] if self.config.DATABASE_URL else 'None'}...")
        
        if self.use_postgres:
            self._init_pool()
    
    def _init_pool(self):
        """コネクションプール初期化"""
        if self.use_postgres and self.config.DATABASE_URL:
            try:
                logger.info("🔌 Creating PostgreSQL connection pool...")
                self.pool = pg_pool.SimpleConnectionPool(
                    1,  # minconn
                    20, # maxconn
                    self.config.DATABASE_URL,
                    connect_timeout=10
                )
                logger.info("✅ PostgreSQL connection pool initialized")
            except Exception as e:
                logger.error(f"❌ Failed to create connection pool: {e}", exc_info=True)
                self.use_postgres = False
                logger.info("⚠️ Falling back to SQLite")
    
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
                
                # プールから接続を取得
                conn = self.pool.getconn()
                
                # ✅ トランザクション状態をリセット（rollbackのみ）
                if conn.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE:
                    try:
                        conn.rollback()
                    except Exception as e:
                        logger.warning(f"⚠️ Rollback during connection reset: {e}")
                
                # 接続が有効かテスト
                if not self._test_connection(conn):
                    logger.warning(f"⚠️ Connection test failed on attempt {attempt + 1}")
                    try:
                        self.pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    raise psycopg2.OperationalError("Connection test failed")
                
                # ✅ autocommit設定を削除（デフォルトのまま使用）
                logger.debug(f"✅ Connection acquired on attempt {attempt + 1}")
                return conn
            
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                logger.warning(f"⚠️ Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                
                if attempt < max_retries - 1:
                    # バックオフ付きでリトライ
                    sleep_time = 0.5 * (2 ** attempt)
                    logger.info(f"⏳ Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    
                    # プールを再初期化
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
        
        # すべてのリトライが失敗
        raise RuntimeError(f"Failed to get database connection after {max_retries} retries: {last_error}")
    
    @contextmanager
    def get_db(self):
        """データベース接続を取得（PostgreSQLは必ずRealDictCursorを使用）"""
        if self.use_postgres:
            conn = None
            try:
                # 再接続処理付きで接続取得
                conn = self._get_connection_with_retry()
                
                # ✅ RealDictCursor用のカスタムコネクションクラス
                class DictConnection:
                    """RealDictCursorを常に返すラッパー"""
                    def __init__(self, real_conn, manager):
                        self._conn = real_conn
                        self._manager = manager
                        self._closed = False
                    
                    def cursor(self, *args, **kwargs):
                        """常にRealDictCursorを返す"""
                        if self._closed:
                            raise psycopg2.InterfaceError("Connection already closed")
                        return self._conn.cursor(cursor_factory=RealDictCursor)
                    
                    def commit(self):
                        if not self._closed:
                            try:
                                return self._conn.commit()
                            except Exception as e:
                                logger.error(f"❌ Commit error: {e}")
                                raise
                    
                    def rollback(self):
                        if not self._closed:
                            try:
                                return self._conn.rollback()
                            except Exception as e:
                                logger.warning(f"⚠️ Rollback error: {e}")
                    
                    def close(self):
                        if not self._closed:
                            self._closed = True
                    
                    def __enter__(self):
                        return self
                    
                    def __exit__(self, exc_type, exc_val, exc_tb):
                        if exc_type:
                            try:
                                self.rollback()
                            except Exception as e:
                                logger.warning(f"⚠️ Error during rollback in __exit__: {e}")
                        else:
                            # ✅ 正常終了時はコミット
                            try:
                                self.commit()
                            except Exception as e:
                                logger.error(f"❌ Error during commit in __exit__: {e}")
                        self.close()
                        return False
                
                wrapped_conn = DictConnection(conn, self)
                logger.debug("✅ PostgreSQL connection with RealDictCursor wrapper")
                
                yield wrapped_conn
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.error(f"❌ Database connection error: {e}", exc_info=True)
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise
            
            except Exception as e:
                logger.error(f"❌ Database error: {e}", exc_info=True)
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise
            
            finally:
                if conn:
                    try:
                        # プールに接続を返却
                        if self.pool:
                            self.pool.putconn(conn)
                            logger.debug("✅ Connection returned to pool")
                    except Exception as e:
                        logger.error(f"❌ Error returning connection to pool: {e}")
        else:
            # SQLite
            conn = sqlite3.connect('portfolio.db', timeout=10.0)
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
    
    def health_check(self):
        """データベース接続の健全性チェック"""
        try:
            with self.get_db() as conn:
                c = conn.cursor()
                c.execute('SELECT 1')
                result = c.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False
    
    def init_database(self):
        """データベーススキーマを初期化"""
        logger.info("📊 Initializing database schema...")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_db() as conn:
                    c = conn.cursor()
                    
                    if self.use_postgres:
                        self._init_postgres(c, conn)
                    else:
                        self._init_sqlite(c, conn)
                    
                    # ✅ 明示的にコミット
                    conn.commit()
                    logger.info("✅ Database schema initialized successfully")
                    return
            
            except Exception as e:
                logger.error(f"❌ Database initialization attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
    
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_type ON assets(user_id, asset_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_id ON asset_history(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_date ON asset_history(user_id, record_date)')
            
            logger.info("✅ PostgreSQL tables created")
            
            # デモユーザー作成
            from werkzeug.security import generate_password_hash
            
            cursor.execute("SELECT id, username FROM users WHERE username = %s", ('demo',))
            existing_demo = cursor.fetchone()
            
            if not existing_demo:
                demo_hash = generate_password_hash('demo123')
                logger.info(f"🔐 Creating demo user")
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                             ('demo', demo_hash))
                logger.info("✅ Demo user created: demo/demo123")
            else:
                logger.info(f"ℹ️ Demo user already exists (ID: {existing_demo['id']})")
            
            logger.info("✅ PostgreSQL database initialized successfully")
        
        except Exception as e:
            logger.error(f"❌ Error initializing PostgreSQL: {e}", exc_info=True)
            raise
    
    def _init_sqlite(self, cursor, conn):
        """SQLite テーブル作成"""
        try:
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
            
            # インデックス作成
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_type ON assets(user_id, asset_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_id ON asset_history(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_date ON asset_history(user_id, record_date)')
            
            logger.info("✅ SQLite tables created")
            
            # デモユーザー作成
            from werkzeug.security import generate_password_hash
            
            cursor.execute("SELECT id FROM users WHERE username = ?", ('demo',))
            if not cursor.fetchone():
                demo_hash = generate_password_hash('demo123')
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                             ('demo', demo_hash))
                logger.info("✅ Demo user created: demo/demo123")
        
        except Exception as e:
            logger.error(f"❌ Error initializing SQLite: {e}", exc_info=True)
            raise
    
    def close_pool(self):
        """コネクションプールをクローズ"""
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("✅ Connection pool closed")
            except Exception as e:
                logger.error(f"❌ Error closing connection pool: {e}")

# グローバルデータベースマネージャー
db_manager = DatabaseManager()
