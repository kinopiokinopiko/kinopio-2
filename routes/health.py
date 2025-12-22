from flask import Blueprint
import requests
import time
import os
from datetime import datetime, timedelta, timezone
from utils import logger
from models import db_manager
# servicesのインポートは関数内で行い循環参照回避

health_bp = Blueprint('health', __name__)

@health_bp.route('/ping')
def ping():
    return "pong", 200

def run_daily_batch():
    """全ユーザーの資産更新・スナップショット保存"""
    logger.info("⏰ Starting Daily Batch (23:58 JST)")
    try:
        from services import price_service, asset_service
        
        # 全ユーザー取得
        with db_manager.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT id, username FROM users')
            users = c.fetchall()
            
        for u in users:
            uid = u[0] if isinstance(u, tuple) else u['id']
            # 資産取得・更新・スナップショット保存...
            # (省略せず実装する場合は asset_service.record_asset_snapshot(uid) を呼ぶだけでOKなように設計推奨)
            # ここでは簡易的にsnapshot呼び出しだけ記載
            try:
                # 価格更新ロジック（assets.pyと同様）を実行してから...
                # asset_service 内に update_user_prices(uid) のような関数を作るとベストですが
                # ここではスナップショット保存を呼び出します
                asset_service.record_asset_snapshot(uid)
                logger.info(f"📸 Snapshot recorded for user {uid}")
            except Exception as e:
                logger.error(f"Error for user {uid}: {e}")
                
    except Exception as e:
        logger.error(f"Batch error: {e}")

def keep_alive():
    """23:58にバッチ実行 & 定期Ping"""
    app_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not app_url:
        logger.warning("RENDER_EXTERNAL_URL not set.")
        return

    ping_url = f"{app_url}/ping"
    last_run = None
    
    logger.info("🚀 Scheduler started.")
    
    while True:
        # Ping
        try:
            requests.get(ping_url, timeout=10)
        except:
            pass
            
        # スケジュール確認 (JST)
        now = datetime.now(timezone(timedelta(hours=9)))
        if now.hour == 23 and now.minute == 58 and last_run != now.date():
            run_daily_batch()
            last_run = now.date()
            
        time.sleep(50) # 1分以内にチェック
