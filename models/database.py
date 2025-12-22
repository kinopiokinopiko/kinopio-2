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
        # Render環境かどうかの判定
        self.is_render = os.environ.get('RENDER') is not None
        
        # PostgreSQLを使用するかどうかの判定
        self.use_postgres = self.config.USE_POSTGRES and POSTGRES_AVAILABLE
        
        logger.info(f"🔧 DatabaseManager initializing...")
        logger.info(f"🌐 Environment: {'Render' if self.is_render else 'Local'}")
        logger.info(f"📊 USE_POSTGRES: {self.use_postgres}")
        
        # DB URLのログ出力（パスワード漏洩防止のため一部伏せ字）
        db_url = self.config.DATABASE_URL
        if db_url:
            masked_url = db_url.split('@')[-1] if '@' in db_url else '***'
            logger.info(f"📊 DATABASE_URL provided (host: {masked_url})")
        else:
            logger.info("📊 DATABASE_URL: None")
        
        # Render環境での構成チェック
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
        """コネクションプール初期化（Neon PostgreSQL最適化版）"""
        # ✅ 修正: 以前のコードでここがインデントエラーになっていました
        if self.use_postgres and self.config.DATABASE_URL:
            try:
                logger.info("🔌 Creating PostgreSQL connection pool (Neon optimized)...")
                # SimpleConnectionPoolを使用（スレッドセーフなアプリケーション構成を前提）
                self.pool = pg_pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=self.config.DATABASE_URL,
                    sslmode='require',            # Render/Neonでは必須
                    connect_timeout=30,           # タイムアウト延長
                    keepalives=1,                 # Keep-alive有効化
                    keepalives_idle=30,           # アイドル30秒後にKA送信
                    keepalives_interval=10,       # KA間隔10秒
                    keepalives_count=5            # KA失敗5回で切断
                )
                logger.info("✅ PostgreSQL connection pool initialized (Neon optimized)")
            except Exception as e:
                logger.error(f"❌ Failed to create connection pool: {e}", exc_info=True)
                # Render環境ではここで落とす
                if self.is_render:
                    raise RuntimeError(f"Failed to initialize PostgreSQL pool: {e}")
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
                
                conn = self.pool.getconn()
                
                # トランザクション状態の確認とリセット
                if conn.status != extensions.TRANSACTION_STATUS_IDLE:
                    try:
                        conn.rollback()
                    except Exception as e:
                        logger.warning(f"⚠️ Rollback during connection reset: {e}")
                
                # 接続テスト
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
        """データベース接続を取得（辞書形式カーソル対応）"""
        if self.use_postgres:
            conn = None
            try:
                conn = self._get_connection_with_retry()
                
                # タイムアウト設定（オプション）
                try:
                    with conn.cursor() as cur:
                        cur.execute('SET statement_timeout = 30000')
                except Exception as e:
                    logger.warning(f"⚠️ Could not set statement_timeout: {e}")

                # 辞書形式でデータを取得するためのラッパークラス
                class DictConnection:
                    def __init__(self, real_conn, manager):
                        self._conn = real_conn
                        self._manager = manager
                        self._closed = False
                    
                    def cursor(self, *args, **kwargs):
                        if self._closed:
                            raise psycopg2.InterfaceError("Connection already closed")
                        # RealDictCursorを強制使用
                        return self._conn.cursor(cursor_factory=RealDictCursor)
                    
                    def commit(self):
                        if not self._closed:
                            return self._conn.commit()
                    
                    def rollback(self):
                        if not self._closed:
                            return self._conn.rollback()
                    
                    def close(self):
                        # ここでは論理的に閉じるだけ
                        if not self._closed:
                            self._closed = True
                    
                    def __enter__(self):
                        return self
                    
                    def __exit__(self, exc_type, exc_val, exc_tb):
                        # コンテキスト終了時にコミットまたはロールバック
                        if exc_type:
                            self.rollback()
                        else:
                            self.commit()
                        self.close()
                        return False
                
                wrapped_conn = DictConnection(conn, self)
                yield wrapped_conn
                
            except Exception as e:
                logger.error(f"❌ Database error: {e}", exc_info=True)
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise
            
            finally:
                if conn and self.pool:
                    try:
                        self.pool.putconn(conn)
                        logger.debug("✅ Connection returned to pool")
                    except Exception as e:
                        logger.error(f"❌ Error returning connection to pool: {e}")
        else:
            # SQLite (ローカル環境用)
            if self.is_render:
                error_msg = "❌ SQLite cannot be used in Render environment!"
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            
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
            
            # ユーザーテーブル
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # 資産テーブル（avg_cost, price, name追加済み）
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
            
            # 履歴テーブル（UPSERT対応のためUNIQUE制約を追加）
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
                CONSTRAINT unique_user_date UNIQUE (user_id, record_date)
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
                logger.info(f"ℹ️ Demo user already exists")
            
            logger.info("✅ PostgreSQL database initialized successfully")
        
        except Exception as e:
            logger.error(f"❌ Error initializing PostgreSQL: {e}", exc_info=True)
            raise
    
    def _init_sqlite(self, cursor, conn):
        """SQLite テーブル作成（ローカル環境用）"""
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
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assets_user_type ON assets(user_id, asset_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_id ON asset_history(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_history_user_date ON asset_history(user_id, record_date)')
            
            logger.info("✅ SQLite tables created")
            
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
