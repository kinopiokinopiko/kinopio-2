import os
import atexit
from flask import Flask
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
    
    # ✅ ルートハンドラーは一切定義しない（auth.pyに完全に委譲）
    
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
