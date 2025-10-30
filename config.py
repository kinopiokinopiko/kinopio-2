import os
from datetime import timedelta

# ================================================================================
# 🔧 アプリケーション設定
# ================================================================================

class Config:
    """基本設定"""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-change-this-in-production')
    FLASK_ENV = os.environ.get('FLASK_ENV', 'development')
    
    # ✅ セッション設定を強化
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    SESSION_COOKIE_SECURE = FLASK_ENV == 'production'
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    SESSION_REFRESH_EACH_REQUEST = False
    
    # キャッシュ設定
    CACHE_DURATION = 300  # 5分
    
    # API タイムアウト
    API_TIMEOUT = 5
    
    # スレッドプール
    MAX_WORKERS = 20
    
    # ✅ データベース設定（Render環境では必須）
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    # ✅ Render環境の検出
    IS_RENDER = os.environ.get('RENDER') is not None
    
    # ✅ postgres:// を postgresql:// に変換
    if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    # ✅ Render環境では DATABASE_URL が必須
    if IS_RENDER:
        if not DATABASE_URL:
            raise RuntimeError(
                "❌ DATABASE_URL is required in Render environment!\n"
                "Please set the DATABASE_URL environment variable in Render dashboard."
            )
        USE_POSTGRES = True
    else:
        # ローカル環境では DATABASE_URL があればPostgreSQL、なければSQLite
        USE_POSTGRES = DATABASE_URL is not None

class DevelopmentConfig(Config):
    """開発環境設定"""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """本番環境設定"""
    DEBUG = False
    TESTING = False

class TestingConfig(Config):
    """テスト環境設定"""
    TESTING = True
    DEBUG = True

# 環境に応じた設定を選択
config_by_env = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig
}

def get_config():
    """環境に応じた設定を取得"""
    env = os.environ.get('FLASK_ENV', 'development')
    return config_by_env.get(env, DevelopmentConfig)
