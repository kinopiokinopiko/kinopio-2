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
    logger.info("⏰ === Starting Daily Batch Process (23:58 JST) ===")
    
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
        
        logger.info(f"👥 Found {len(users)} users for daily update.")
        
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
                
                # 3. スナップショット保存（UPSERT処理が前提）
                asset_service.record_asset_snapshot(user_id)
                logger.info(f"   📸 Snapshot recorded for {username}")
                
            except Exception as e:
                logger.error(f"   ❌ Error processing user {username}: {e}")
                continue
                
        logger.info("✅ === Daily Batch Process Completed ===")
        
    except Exception as e:
        logger.error(f"❌ Critical Error in Daily Batch: {e}", exc_info=True)

def keep_alive():
    """
    アプリケーションがスリープしないように定期的にPingを送り、
    かつ23:58(JST)になったらバッチ処理を実行するループ関数。
    """
    app_url = os.environ.get('RENDER_EXTERNAL_URL')
    
    # URLが設定されていなくてもループ（スケジューラ）は回す場合、ここを調整
    if not app_url:
        logger.info("⚠️ RENDER_EXTERNAL_URL is not set. Keep-alive ping will not run, but scheduler might need this thread.")
        # 必要に応じて return せず、ping_url = None として扱う
    
    ping_url = f"{app_url}/ping" if app_url else None
    last_run_date = None
    
    logger.info("🚀 Keep-alive & Scheduler thread started.")
    
    while True:
        # 1. Ping送信 (Sleep防止)
        if ping_url:
            try:
                requests.get(ping_url, timeout=10)
            except Exception as e:
                logger.error(f"Keep-alive ping failed: {e}")
        
        # 2. 定期実行チェック (JST)
        now_jst = datetime.now(timezone(timedelta(hours=9)))
        current_date = now_jst.date()
        
        # 23:58台 かつ 今日まだ実行していない場合
        if now_jst.hour == 23 and now_jst.minute == 58 and last_run_date != current_date:
            logger.info(f"⏰ It is {now_jst.strftime('%H:%M')}. Running daily batch...")
            try:
                run_daily_batch()
                last_run_date = current_date
            except Exception as e:
                logger.error(f"Scheduler execution failed: {e}")
        
        # 23:58を逃さないよう、短めの間隔で待機 (50秒)
        time.sleep(50)
