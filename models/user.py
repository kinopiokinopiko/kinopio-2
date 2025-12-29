# -*- coding: utf-8 -*-

"""
================================================================================
👤 models/user.py - ユーザーモデル
================================================================================

ユーザーデータモデルとパスワード管理を提供
"""

from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone, timedelta
from utils import logger

# ================================================================================
# 👤 ユーザーモデル
# ================================================================================

class User:
    """ユーザークラス"""
    
    def __init__(self, id, username, password_hash, created_at=None):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.created_at = created_at or self._get_current_time()
    
    @staticmethod
    def _get_current_time():
        """現在時刻を取得（JST）"""
        jst = timezone(timedelta(hours=9))
        return datetime.now(jst)
    
    def set_password(self, password):
        """パスワードをハッシュ化して設定"""
        if not password or len(password) < 6:
            raise ValueError("Password must be at least 6 characters long")
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """入力されたパスワードが正しいか確認"""
        if not password or not self.password_hash:
            logger.error(f"❌ Password check failed: password={bool(password)}, hash={bool(self.password_hash)}")
            return False
        try:
            logger.info(f"🔑 Checking password for user {self.username}")
            logger.info(f"🔑 Hash preview: {self.password_hash[:50]}...")
            result = check_password_hash(self.password_hash, password)
            logger.info(f"🔑 Password check result: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ Error checking password: {e}", exc_info=True)
            return False
    
    def to_dict(self):
        """ユーザー情報を辞書形式で返す"""
        return {
            'id': self.id,
            'username': self.username,
            'created_at': str(self.created_at)
        }
    
    def __repr__(self):
        return f"<User {self.id}: {self.username}>"

# ================================================================================
# 🔧 ユーティリティ関数
# ================================================================================

def row_to_dict(row):
    """SQLite Row または PostgreSQL の dict-like オブジェクトを dict に変換"""
    if row is None:
        logger.error("❌ row_to_dict: row is None")
        return None
    
    try:
        logger.info(f"🔍 row_to_dict: row type = {type(row)}")
        
        # PostgreSQL の RealDictCursor の場合（既に辞書型）
        if isinstance(row, dict):
            logger.info(f"✅ row_to_dict: Already a dict with keys: {list(row.keys())}")
            return row
        
        # SQLite の Row オブジェクトまたは psycopg2 の tuple-like オブジェクト
        if hasattr(row, 'keys'):
            result = dict(zip(row.keys(), row))
            logger.info(f"✅ row_to_dict: Converted to dict with keys: {list(result.keys())}")
            return result
        
        # その他のタプル形式
        result = dict(row) if hasattr(row, '__iter__') else row
        logger.info(f"✅ row_to_dict: Fallback conversion, type: {type(result)}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error converting row to dict: {e}, row type: {type(row)}", exc_info=True)
        return None

# ================================================================================
# 🔐 ユーザーサービス（DB操作）
# ================================================================================

class UserService:
    """ユーザー関連のDB操作を管理"""
    
    def __init__(self, db_manager, use_postgres=False):
        self.db_manager = db_manager
        self.use_postgres = use_postgres
        logger.info(f"🔧 UserService initialized: use_postgres={use_postgres}")
    
    def _get_user_columns(self):
        """使用可能なカラムを取得"""
        return "id, username, password_hash"
    
    def get_user_by_id(self, user_id):
        """IDでユーザーを取得"""
        try:
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute(f'SELECT {self._get_user_columns()} FROM users WHERE id = %s', (user_id,))
                else:
                    c.execute(f'SELECT {self._get_user_columns()} FROM users WHERE id = ?', (user_id,))
                
                row = c.fetchone()
                
                if row:
                    row_dict = row_to_dict(row)
                    if row_dict:
                        return User(
                            row_dict['id'],
                            row_dict['username'],
                            row_dict['password_hash']
                        )
                return None
        except Exception as e:
            logger.error(f"❌ Error getting user by id: {e}", exc_info=True)
            return None
    
    def get_user_by_username(self, username):
        """ユーザー名でユーザーを取得"""
        try:
            logger.info(f"🔍 Searching for user: {username}")
            logger.info(f"🔍 Database mode: {'PostgreSQL' if self.use_postgres else 'SQLite'}")
            
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute(f'SELECT {self._get_user_columns()} FROM users WHERE username = %s', (username,))
                else:
                    c.execute(f'SELECT {self._get_user_columns()} FROM users WHERE username = ?', (username,))
                
                row = c.fetchone()
                
                if row is None:
                    logger.warning(f"❌ User not found in database: {username}")
                    # デバッグ: 全ユーザーを表示
                    if self.use_postgres:
                        c.execute('SELECT username FROM users')
                    else:
                        c.execute('SELECT username FROM users')
                    all_users = [r[0] if isinstance(r, tuple) else r['username'] for r in c.fetchall()]
                    logger.info(f"📋 Available users in DB: {all_users}")
                    return None
                
                logger.info(f"✅ Row fetched for {username}, type: {type(row)}")
                
                row_dict = row_to_dict(row)
                if not row_dict:
                    logger.error(f"❌ Failed to convert row to dict for user: {username}")
                    return None
                
                logger.info(f"✅ User dict created with keys: {list(row_dict.keys())}")
                logger.info(f"✅ User ID: {row_dict.get('id')}, Username: {row_dict.get('username')}")
                logger.info(f"🔑 Password hash preview: {row_dict.get('password_hash', '')[:50]}...")
                
                user = User(
                    row_dict['id'],
                    row_dict['username'],
                    row_dict['password_hash']
                )
                
                logger.info(f"✅ User object created: {user}")
                return user
                
        except Exception as e:
            logger.error(f"❌ Error getting user by username: {e}", exc_info=True)
            return None
    
    def create_user(self, username, password):
        """新規ユーザーを作成"""
        try:
            logger.info(f"👤 Creating user: {username}")
            
            # バリデーション
            if not username or len(username) < 3:
                raise ValueError("Username must be at least 3 characters")
            
            if not password or len(password) < 6:
                raise ValueError("Password must be at least 6 characters")
            
            # ユーザーが既に存在するか確認
            existing_user = self.get_user_by_username(username)
            if existing_user:
                logger.warning(f"⚠️ User already exists: {username}")
                raise ValueError("Username already exists")
            
            # パスワードをハッシュ化
            password_hash = generate_password_hash(password)
            logger.info(f"🔐 Password hashed for user: {username}, hash preview: {password_hash[:50]}...")
            
            # DBに保存
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute(
                        'INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id',
                        (username, password_hash)
                    )
                    result = c.fetchone()
                    new_user_id = result[0] if result else None
                else:
                    c.execute(
                        'INSERT INTO users (username, password_hash) VALUES (?, ?)',
                        (username, password_hash)
                    )
                    new_user_id = c.lastrowid
                
                conn.commit()
            
            logger.info(f"✅ User created: {username} (ID: {new_user_id})")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error creating user: {e}", exc_info=True)
            raise
    
    def verify_user(self, username, password):
        """ユーザーの認証"""
        try:
            logger.info(f"🔐 === Starting verification for user: {username} ===")
            
            user = self.get_user_by_username(username)
            
            if not user:
                logger.warning(f"❌ Verification failed: user not found - {username}")
                return False
            
            logger.info(f"✅ User object retrieved: {user}")
            logger.info(f"🔑 User has password_hash: {bool(user.password_hash)}")
            
            # パスワードチェック
            is_valid = user.check_password(password)
            logger.info(f"🔑 Final verification result: {'✅ VALID' if is_valid else '❌ INVALID'} for user {username}")
            logger.info(f"🔐 === Verification complete for user: {username} ===")
            
            return is_valid
        except Exception as e:
            logger.error(f"❌ Error verifying user {username}: {e}", exc_info=True)
            return False
    
    def update_password(self, user_id, old_password, new_password):
        """パスワードを更新"""
        try:
            user = self.get_user_by_id(user_id)
            
            if not user:
                raise ValueError("User not found")
            
            if not user.check_password(old_password):
                raise ValueError("Old password is incorrect")
            
            if len(new_password) < 6:
                raise ValueError("New password must be at least 6 characters")
            
            user.set_password(new_password)
            
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute(
                        'UPDATE users SET password_hash = %s WHERE id = %s',
                        (user.password_hash, user_id)
                    )
                else:
                    c.execute(
                        'UPDATE users SET password_hash = ? WHERE id = ?',
                        (user.password_hash, user_id)
                    )
                
                conn.commit()
            
            logger.info(f"✅ Password updated for user {user_id}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error updating password: {e}", exc_info=True)
            raise
    
    def delete_user(self, user_id):
        """ユーザーを削除"""
        try:
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute('DELETE FROM users WHERE id = %s', (user_id,))
                else:
                    c.execute('DELETE FROM users WHERE id = ?', (user_id,))
                
                conn.commit()
            
            logger.info(f"✅ User deleted: {user_id}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error deleting user: {e}", exc_info=True)
            raise
    
    def get_all_users(self):
        """すべてのユーザーを取得"""
        try:
            with self.db_manager.get_db() as conn:
                c = conn.cursor()
                c.execute(f'SELECT {self._get_user_columns()} FROM users ORDER BY id DESC')
                rows = c.fetchall()
                
                users = []
                for row in rows:
                    row_dict = row_to_dict(row)
                    if row_dict:
                        user = User(
                            row_dict['id'],
                            row_dict['username'],
                            row_dict['password_hash']
                        )
                        users.append(user)
                
                return users
        except Exception as e:
            logger.error(f"❌ Error getting all users: {e}", exc_info=True)
            return []
