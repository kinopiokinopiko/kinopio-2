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
    """Keep-Alive を管理（10分ごとにpingを送信）"""
    
    def __init__(self):
        self.session = requests.Session()
        self.running = False
        self.thread = None
    
    def keep_alive(self):
        """アプリケーションがスリープしないようにping（10分ごと）"""
        app_url = os.environ.get('RENDER_EXTERNAL_URL')
        
        if not app_url:
            logger.warning("⚠️ RENDER_EXTERNAL_URL is not set. Keep-alive will not run.")
            logger.info("ℹ️ Set RENDER_EXTERNAL_URL environment variable on Render dashboard")
            return
        
        # URLの末尾のスラッシュを削除
        app_url = app_url.rstrip('/')
        ping_url = f"{app_url}/ping"
        
        logger.info(f"🚀 Keep-alive thread started")
        logger.info(f"📡 Ping URL: {ping_url}")
        logger.info(f"⏱️ Interval: 10 minutes (600 seconds)")
        
        while self.running:
            try:
                logger.info(f"📡 Sending keep-alive ping to {ping_url}...")
                response = self.session.get(ping_url, timeout=10)
                
                if response.status_code == 200:
                    logger.info(f"✅ Keep-alive ping successful (Status: {response.status_code})")
                else:
                    logger.warning(f"⚠️ Keep-alive ping returned status {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Keep-alive ping timeout after 10 seconds")
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Keep-alive ping failed: {e}")
            except Exception as e:
                logger.error(f"❌ Unexpected error in keep-alive: {e}", exc_info=True)
            
            # 10分（600秒）待機
            time.sleep(600)
    
    def start_thread(self):
        """Keep-Alive スレッドを開始"""
        # Render環境でのみ実行
        if os.environ.get('RENDER'):
            logger.info("🌐 Running on Render, starting keep-alive thread...")
            
            # 既に実行中の場合はスキップ
            if self.running:
                logger.info("ℹ️ Keep-alive thread already running")
                return
            
            self.running = True
            self.thread = threading.Thread(target=self.keep_alive, daemon=True, name="KeepAliveThread")
            self.thread.start()
            logger.info("✅ Keep-alive thread started successfully")
        else:
            logger.info("ℹ️ Not running on Render, keep-alive thread will not start")
            logger.info("ℹ️ (This is normal for local development)")
    
    def stop(self):
        """Keep-Alive スレッドを停止"""
        if self.running:
            logger.info("🛑 Stopping keep-alive thread...")
            self.running = False
            if self.thread:
                self.thread.join(timeout=5)
            logger.info("✅ Keep-alive thread stopped")

# グローバルインスタンス
scheduler_manager = SchedulerManager()
keep_alive_manager = KeepAliveManager()
