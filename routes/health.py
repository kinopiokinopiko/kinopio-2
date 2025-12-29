from flask import Blueprint
import requests
import time
import os
import threading
from datetime import datetime, timedelta, timezone
from utils import logger
from models import db_manager

# ================================================================================
# 🏥 ヘルスチェック & 自動更新スケジューラー
# ================================================================================

health_bp = Blueprint('health', __name__)

@health_bp.route('/ping')
def ping():
    """スリープ防止用のエンドポイント"""
    return "pong", 200

def run_daily_batch():
    """全ユーザーの資産更新・スナップショット保存を行うバッチ処理"""
    logger.info("⏰ === Starting Daily Batch Process (Manual Trigger) ===")
    
    try:
        # 循環参照を避けるため関数内でインポート
        from services import price_service, asset_service
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('SELECT id, username FROM users')
            else:
                c.execute('SELECT id, username FROM users')
            users = c.fetchall()
        
        logger.info(f"👥 Found {len(users)} users for update.")
        
        for user in users:
            user_id = user['id']
            username = user['username']
            logger.info(f"🔄 Processing user: {username} (ID: {user_id})")
            
            try:
                # 1. 更新対象の資産を取得
                with db_manager.get_db() as conn:
                    c = conn.cursor()
                    asset_types = ['jp_stock', 'us_stock', 'gold', 'crypto', 'investment_trust']
                    ph = ', '.join(['%s'] * len(asset_types)) if db_manager.use_postgres else ', '.join(['?'] * len(asset_types))
                    query = f"SELECT id, asset_type, symbol FROM assets WHERE user_id = {('%s' if db_manager.use_postgres else '?')} AND asset_type IN ({ph})"
                    params = [user_id] + asset_types
                    c.execute(query, params)
                    assets = c.fetchall()
                
                if assets:
                    # 2. 価格更新
                    assets_list = [{'id': int(a['id']), 'asset_type': str(a['asset_type']), 'symbol': str(a['symbol'])} for a in assets]
                    updated_prices = price_service.fetch_prices_parallel(assets_list)
                    
                    if updated_prices:
                        with db_manager.get_db() as conn:
                            c = conn.cursor()
                            for p in updated_prices:
                                if db_manager.use_postgres:
                                    c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s', (float(p['price']), str(p.get('name','')), int(p['id'])))
                                else:
                                    c.execute('UPDATE assets SET price = ?, name = ? WHERE id = ?', (float(p['price']), str(p.get('name','')), int(p['id'])))
                            conn.commit()
                        logger.info(f"   ✅ Prices updated for {username}")
                
                # 3. スナップショット保存
                asset_service.record_asset_snapshot(user_id)
                logger.info(f"   📸 Snapshot recorded for {username}")
                
            except Exception as e:
                logger.error(f"   ❌ Error processing user {username}: {e}")
                continue
                
        logger.info("✅ === Batch Process Completed ===")
        
    except Exception as e:
        logger.error(f"❌ Critical Error in Batch: {e}", exc_info=True)

def keep_alive():
    """
    アプリケーションがスリープしないように定期的にPingを送るループ関数。
    ※重要: バッチ処理の自動実行は scheduler_service.py に任せるため、ここでは実行しません。
    """
    app_url = os.environ.get('RENDER_EXTERNAL_URL')
    
    if not app_url:
        logger.info("⚠️ RENDER_EXTERNAL_URL is not set. Keep-alive ping will not run.")
        # ループは継続しない（スレッド終了）
        return
    
    ping_url = f"{app_url}/ping"
    logger.info("🚀 Keep-alive thread started.")
    
    while True:
        # 1. Ping送信 (Sleep防止)
        try:
            requests.get(ping_url, timeout=10)
        except Exception as e:
            logger.error(f"Keep-alive ping failed: {e}")
        
        # 2. 待機 (5分間隔)
        time.sleep(300)
