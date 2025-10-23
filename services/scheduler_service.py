import os
import time
import threading
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from utils import logger
from models import db_manager
from .asset_service import asset_service
from config import get_config

# ================================================================================
# ⏰ スケジューラー関連
# ================================================================================

class SchedulerManager:
    """スケジューラーを管理"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone='Asia/Tokyo')
        self.config = get_config()
        self.use_postgres = self.config.USE_POSTGRES
    
    def scheduled_update_all_prices(self):
        """スケジュール実行: 全ユーザーの資産価格を更新"""
        try:
            logger.info("=" * 70)
            logger.info("🔄 Starting scheduled price update for all users")
            logger.info("=" * 70)
            
            with db_manager.get_db() as conn:
                c = conn.cursor()
                c.execute('SELECT id, username FROM users')
                users = c.fetchall()
            
            if not users:
                logger.warning("No users found in database")
                return
            
            logger.info(f"Found {len(users)} users to update")
            
            total_updated = 0
            for user in users:
                user_id = user['id']
                username = user['username']
                
                logger.info(f"👤 Processing user: {username} (ID: {user_id})")
                
                updated_count = asset_service.update_user_prices(user_id)
                total_updated += updated_count
                
                try:
                    asset_service.record_asset_snapshot(user_id)
                    logger.info(f"📸 Asset snapshot recorded for user {username}")
                except Exception as e:
                    logger.error(f"Failed to record snapshot for user {username}: {e}")
            
            logger.info("=" * 70)
            logger.info(f"✅ Scheduled update completed: {total_updated} assets updated across {len(users)} users")
            logger.info("=" * 70)
        
        except Exception as e:
            logger.error(f"❌ Critical error in scheduled_update_all_prices: {e}", exc_info=True)
    
    def start(self):
        """スケジューラーを開始"""
        self.scheduler.add_job(
            func=self.scheduled_update_all_prices,
            trigger=CronTrigger(hour=23, minute=58, timezone='Asia/Tokyo'),
            id='daily_price_update',
            name='Daily Price Update at 23:58 JST',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        
        try:
            self.scheduler.start()
            logger.info("✅ Scheduler started successfully. Daily updates scheduled for 23:58 JST")
        except Exception as e:
            logger.error(f"❌ Failed to start scheduler: {e}")
    
    def shutdown(self):
        """スケジューラーをシャットダウン"""
        try:
            self.scheduler.shutdown()
            logger.info("✅ Scheduler shutdown successfully")
        except Exception as e:
            logger.error(f"❌ Failed to shutdown scheduler: {e}")

class KeepAliveManager:
    """Keep-Alive を管理"""
    
    def __init__(self):
        self.session = requests.Session()
    
    def keep_alive(self):
        """アプリケーションがスリープしないようにping"""
        app_url = os.environ.get('RENDER_EXTERNAL_URL')
        
        if not app_url:
            logger.info("RENDER_EXTERNAL_URL is not set. Keep-alive thread will not run.")
            return
        
        ping_url = f"{app_url}/ping"
        
        while True:
            try:
                logger.info(f"📡 Sending keep-alive ping...")
                response = self.session.get(ping_url, timeout=5)
                logger.info(f"✅ Keep-alive ping successful. Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Keep-alive ping failed: {e}")
            
            time.sleep(600)  # 10分ごと
    
    def start_thread(self):
        """Keep-Alive スレッドを開始"""
        if os.environ.get('RENDER'):
            logger.info("🚀 Starting keep-alive thread for Render...")
            thread = threading.Thread(target=self.keep_alive, daemon=True)
            thread.start()

# グローバルインスタンス
scheduler_manager = SchedulerManager()
keep_alive_manager = KeepAliveManager()