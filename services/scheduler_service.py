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
        self.session = requests.Session()
    
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
    
    def _self_ping(self):
        """定期的に自身にpingを送信してスリープを防止"""
        app_url = os.environ.get('RENDER_EXTERNAL_URL')
        
        if not app_url:
            logger.debug("ℹ️ RENDER_EXTERNAL_URL not set, skipping self-ping")
            return
        
        ping_url = f"{app_url.rstrip('/')}/ping"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"📡 Self-ping attempt {attempt + 1}/{max_retries} to {ping_url}")
                response = self.session.get(ping_url, timeout=10)
                
                if response.status_code == 200:
                    logger.info(f"✅ Self-ping successful (Status: {response.status_code})")
                    return
                else:
                    logger.warning(f"⚠️ Self-ping returned status {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Self-ping timeout on attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Self-ping failed on attempt {attempt + 1}: {e}")
            except Exception as e:
                logger.error(f"❌ Unexpected error in self-ping attempt {attempt + 1}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2)
        
        logger.error(f"❌ All {max_retries} self-ping attempts failed")
    
    def start(self):
        """スケジューラーを開始"""
        # 毎日23:58に価格更新
        self.scheduler.add_job(
            func=self.scheduled_update_all_prices,
            trigger=CronTrigger(hour=23, minute=58, timezone='Asia/Tokyo'),
            id='daily_price_update',
            name='Daily Price Update at 23:58 JST',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        
        # ✅ 5分ごとにself-pingを送信（スリープ防止）
        self.scheduler.add_job(
            func=self._self_ping,
            trigger=CronTrigger(minute='*/5', timezone='Asia/Tokyo'),
            id='self_ping_job',
            name='Self Ping every 5 minutes',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        
        try:
            self.scheduler.start()
            logger.info("✅ Scheduler started successfully")
            logger.info("📅 Daily price update scheduled for 23:58 JST")
            logger.info("📡 Self-ping scheduled every 5 minutes")
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
    """Keep-Alive を管理（5分ごとにpingを送信）"""
    
    def __init__(self):
        self.session = requests.Session()
        self.running = False
        self.thread = None
    
    def keep_alive(self):
        """アプリケーションがスリープしないようにping（5分ごと）"""
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
        logger.info(f"⏱️ Interval: 5 minutes (300 seconds)")
        
        while self.running:
            max_retries = 3
            success = False
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"📡 Keep-alive ping attempt {attempt + 1}/{max_retries}...")
                    response = self.session.get(ping_url, timeout=10)
                    
                    if response.status_code == 200:
                        logger.info(f"✅ Keep-alive ping successful (Status: {response.status_code})")
                        success = True
                        break
                    else:
                        logger.warning(f"⚠️ Keep-alive ping returned status {response.status_code}")
                        
                except requests.exceptions.Timeout:
                    logger.warning(f"⚠️ Keep-alive ping timeout on attempt {attempt + 1}")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Keep-alive ping failed on attempt {attempt + 1}: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error in keep-alive attempt {attempt + 1}: {e}", exc_info=True)
                
                if attempt < max_retries - 1:
                    time.sleep(2)
            
            if not success:
                logger.error(f"❌ All {max_retries} keep-alive attempts failed")
            
            # ✅ 5分（300秒）待機
            time.sleep(300)
    
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
            logger.info("✅ Keep-alive thread started successfully (5-minute interval)")
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
