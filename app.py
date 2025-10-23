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
    
    # ロギング
    logger.info("🚀 Creating Flask application...")
    
    # データベース初期化
    db_manager.init_database()
    logger.info("✅ Database initialized")
    
    # Blueprintを登録
    register_blueprints(app)
    logger.info("✅ Blueprints registered")
    
    # スケジューラーを開始
    scheduler_manager.start()
    
    # Keep-Aliveを開始
    keep_alive_manager.start_thread()
    
    # アプリ終了時にスケジューラーをシャットダウン
    def shutdown():
        logger.info("Shutting down scheduler...")
        scheduler_manager.shutdown()
    
    atexit.register(shutdown)
    
    logger.info("✅ Application created successfully")
    
    return app

# アプリケーションインスタンスを作成
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)