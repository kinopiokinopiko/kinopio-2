import os
import atexit
from flask import Flask, redirect, url_for, session
from config import get_config
from models import db_manager
from services import scheduler_manager, keep_alive_manager
from routes import register_blueprints
from utils import logger

# ================================================================================
# 🚀 メインアプリケーション
# ================================================================================

def create_app(config=None):
    """Flask アプリケーションファクトリ"""
    
    app = Flask(__name__)
    
    # 設定を読み込み
    if config is None:
        config = get_config()
    app.config.from_object(config)
    
    # ✅ 修正: SECRET_KEYが正しく設定されているか確認
    if not app.config.get('SECRET_KEY') or app.config['SECRET_KEY'] == 'your-secret-key-change-this-in-production':
        logger.warning("⚠️ Using default SECRET_KEY. Please set SECRET_KEY environment variable!")
        # Renderで自動生成されたSECRET_KEYを使用
        import secrets
        app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
    
    # ロギング設定
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=" * 70)
    logger.info("🚀 Creating Flask application...")
    logger.info(f"📊 Environment: {config.FLASK_ENV}")
    logger.info(f"📊 Database: {'PostgreSQL' if config.USE_POSTGRES else 'SQLite'}")
    logger.info(f"📊 Database URL: {config.DATABASE_URL[:30]}..." if config.DATABASE_URL else "📊 Database URL: None")
    logger.info(f"📊 Secret Key: {app.config['SECRET_KEY'][:10]}...")
    logger.info("=" * 70)
    
    # データベース初期化
    try:
        db_manager.init_database()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}", exc_info=True)
        raise
    
    # Blueprintを登録
    try:
        register_blueprints(app)
        logger.info("✅ Blueprints registered")
    except Exception as e:
        logger.error(f"❌ Blueprint registration failed: {e}", exc_info=True)
        raise
    
    # ✅ ルートパスへのアクセスをログインページにリダイレクト
    @app.route('/')
    def index():
        # セッションがあればダッシュボードへ
        if 'user_id' in session:
            logger.info(f"✅ User {session.get('username')} accessing root, redirecting to dashboard")
            return redirect(url_for('dashboard.dashboard'))
        logger.info("📍 Root path accessed, redirecting to login")
        return redirect(url_for('auth.login'))
    
    # ✅ デバッグ用: セッション確認エンドポイント
    @app.route('/debug/session')
    def debug_session():
        return {
            'user_id': session.get('user_id'),
            'username': session.get('username'),
            'session_keys': list(session.keys())
        }
    
    # エラーハンドラ
    @app.errorhandler(404)
    def not_found(e):
        logger.warning(f"404 Error: {e}")
        return redirect(url_for('auth.login'))
    
    @app.errorhandler(500)
    def server_error(e):
        logger.error(f"500 Error: {e}", exc_info=True)
        return "Internal Server Error", 500
    
    # スケジューラーを開始
    try:
        scheduler_manager.start()
        logger.info("✅ Scheduler started")
    except Exception as e:
        logger.warning(f"⚠️ Scheduler start failed: {e}")
    
    # Keep-Aliveを開始
    try:
        keep_alive_manager.start_thread()
        logger.info("✅ Keep-alive thread started")
    except Exception as e:
        logger.warning(f"⚠️ Keep-alive start failed: {e}")
    
    # アプリ終了時にスケジューラーをシャットダウン
    def shutdown():
        logger.info("🛑 Shutting down scheduler...")
        try:
            scheduler_manager.shutdown()
        except Exception as e:
            logger.error(f"❌ Scheduler shutdown error: {e}")
    
    atexit.register(shutdown)
    
    logger.info("=" * 70)
    logger.info("✅ Application created successfully")
    logger.info("=" * 70)
    
    return app

# アプリケーションインスタンスを作成
app = create_app()

# デバッグ情報を出力
if __name__ == '__main__':
    logger.info("🏃 Running in development mode")
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    logger.info("🚀 Running with Gunicorn in production mode")
