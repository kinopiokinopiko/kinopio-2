from datetime import datetime, timezone, timedelta
import time
from utils import logger
from models import db_manager
from config import get_config
from .price_service import price_service

# ================================================================================
# 💼 資産管理サービス
# ================================================================================

class AssetService:
    """資産管理のビジネスロジック"""
    
    def __init__(self):
        self.config = get_config()
        self.use_postgres = self.config.USE_POSTGRES
    
    def record_asset_snapshot(self, user_id):
        """現在の資産状況をスナップショットとして記録（前日比を含む） - リトライ機能付き"""
        
        max_retries = 3
        retry_delay = 1.0 # 秒
        
        for attempt in range(max_retries):
            try:
                logger.info(f"📸 === Starting asset snapshot for user {user_id} (Attempt {attempt+1}/{max_retries}) ===")
                
                with db_manager.get_db() as conn:
                    # PostgreSQL/SQLiteの統一インターフェース
                    if self.use_postgres:
                        from psycopg2.extras import RealDictCursor
                        c = conn.cursor(cursor_factory=RealDictCursor)
                    else:
                        c = conn.cursor()
                    
                    jst = timezone(timedelta(hours=9))
                    today = datetime.now(jst).date()
                    yesterday = today - timedelta(days=1)
                    
                    logger.info(f"📅 Recording snapshot for date: {today}")
                    
                    asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
                    values = {}
                    
                    # USD/JPYレートを取得
                    try:
                        usd_jpy = price_service.get_usd_jpy_rate()
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get USD/JPY rate: {e}")
                        usd_jpy = 150.0
                    
                    # 当日の資産値を計算
                    for asset_type in asset_types:
                        if self.use_postgres:
                            c.execute('SELECT * FROM assets WHERE user_id = %s AND asset_type = %s',
                                     (user_id, asset_type))
                        else:
                            c.execute('SELECT * FROM assets WHERE user_id = ? AND asset_type = ?',
                                     (user_id, asset_type))
                        assets = c.fetchall()
                        
                        total = 0
                        if asset_type == 'us_stock':
                            total = sum(float(a['quantity'] or 0) * float(a['price'] or 0) for a in assets) * usd_jpy
                        elif asset_type == 'investment_trust':
                            total = sum((float(a['quantity'] or 0) * float(a['price'] or 0) / 10000) for a in assets)
                        elif asset_type == 'insurance':
                            total = sum(float(a['price'] or 0) for a in assets)
                        elif asset_type == 'cash':
                            total = sum(float(a['quantity'] or 0) for a in assets)
                        else:
                            total = sum(float(a['quantity'] or 0) * float(a['price'] or 0) for a in assets)
                        
                        values[asset_type] = total
                    
                    total_value = sum(values.values())
                    
                    # 昨日のスナップショットを取得（前日の値として使用）
                    if self.use_postgres:
                        c.execute('''SELECT jp_stock_value, us_stock_value, cash_value, 
                                            gold_value, crypto_value, investment_trust_value, 
                                            insurance_value, total_value 
                                    FROM asset_history 
                                    WHERE user_id = %s AND record_date = %s''',
                                 (user_id, yesterday))
                    else:
                        c.execute('''SELECT jp_stock_value, us_stock_value, cash_value, 
                                            gold_value, crypto_value, investment_trust_value, 
                                            insurance_value, total_value 
                                    FROM asset_history 
                                    WHERE user_id = ? AND record_date = ?''',
                                 (user_id, yesterday))
                    
                    yesterday_record = c.fetchone()
                    
                    # 前日のデータがある場合はそれを使用、ない場合は0
                    if yesterday_record:
                        prev_values = {
                            'jp_stock': float(yesterday_record['jp_stock_value'] or 0),
                            'us_stock': float(yesterday_record['us_stock_value'] or 0),
                            'cash': float(yesterday_record['cash_value'] or 0),
                            'gold': float(yesterday_record['gold_value'] or 0),
                            'crypto': float(yesterday_record['crypto_value'] or 0),
                            'investment_trust': float(yesterday_record['investment_trust_value'] or 0),
                            'insurance': float(yesterday_record['insurance_value'] or 0),
                        }
                        prev_total_value = float(yesterday_record['total_value'] or 0)
                    else:
                        prev_values = {
                            'jp_stock': values['jp_stock'],
                            'us_stock': values['us_stock'],
                            'cash': values['cash'],
                            'gold': values['gold'],
                            'crypto': values['crypto'],
                            'investment_trust': values['investment_trust'],
                            'insurance': values['insurance'],
                        }
                        prev_total_value = total_value
                    
                    # 当日のスナップショットを保存または更新
                    if self.use_postgres:
                        # PostgreSQLの場合：UPSERT（ON CONFLICT）を使用
                        c.execute('''INSERT INTO asset_history 
                                    (user_id, record_date, jp_stock_value, us_stock_value, cash_value, 
                                     gold_value, crypto_value, investment_trust_value, insurance_value, total_value,
                                     prev_jp_stock_value, prev_us_stock_value, prev_cash_value,
                                     prev_gold_value, prev_crypto_value, prev_investment_trust_value,
                                     prev_insurance_value, prev_total_value)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id, record_date) 
                                    DO UPDATE SET 
                                        jp_stock_value = EXCLUDED.jp_stock_value,
                                        us_stock_value = EXCLUDED.us_stock_value,
                                        cash_value = EXCLUDED.cash_value,
                                        gold_value = EXCLUDED.gold_value,
                                        crypto_value = EXCLUDED.crypto_value,
                                        investment_trust_value = EXCLUDED.investment_trust_value,
                                        insurance_value = EXCLUDED.insurance_value,
                                        total_value = EXCLUDED.total_value,
                                        prev_jp_stock_value = EXCLUDED.prev_jp_stock_value,
                                        prev_us_stock_value = EXCLUDED.prev_us_stock_value,
                                        prev_cash_value = EXCLUDED.prev_cash_value,
                                        prev_gold_value = EXCLUDED.prev_gold_value,
                                        prev_crypto_value = EXCLUDED.prev_crypto_value,
                                        prev_investment_trust_value = EXCLUDED.prev_investment_trust_value,
                                        prev_insurance_value = EXCLUDED.prev_insurance_value,
                                        prev_total_value = EXCLUDED.prev_total_value''',
                                 (user_id, today, values['jp_stock'], values['us_stock'], values['cash'],
                                  values['gold'], values['crypto'], values['investment_trust'], values['insurance'], 
                                  total_value,
                                  prev_values['jp_stock'], prev_values['us_stock'], prev_values['cash'],
                                  prev_values['gold'], prev_values['crypto'], prev_values['investment_trust'],
                                  prev_values['insurance'], prev_total_value))
                    else:
                        # SQLiteの場合
                        c.execute('''INSERT OR REPLACE INTO asset_history 
                                    (user_id, record_date, jp_stock_value, us_stock_value, cash_value, 
                                     gold_value, crypto_value, investment_trust_value, insurance_value, total_value,
                                     prev_jp_stock_value, prev_us_stock_value, prev_cash_value,
                                     prev_gold_value, prev_crypto_value, prev_investment_trust_value,
                                     prev_insurance_value, prev_total_value)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                 (user_id, today, values['jp_stock'], values['us_stock'], values['cash'],
                                  values['gold'], values['crypto'], values['investment_trust'], values['insurance'], 
                                  total_value,
                                  prev_values['jp_stock'], prev_values['us_stock'], prev_values['cash'],
                                  prev_values['gold'], prev_values['crypto'], prev_values['investment_trust'],
                                  prev_values['insurance'], prev_total_value))
                    
                    conn.commit()
                    logger.info(f"✅ Asset snapshot completed for user {user_id}")
                    return # 成功したら終了
                
            except Exception as e:
                logger.error(f"⚠️ Error recording snapshot (Attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    logger.error(f"❌ Failed to record asset snapshot after {max_retries} attempts")
                    raise
    
    def update_user_prices(self, user_id):
        """特定ユーザーの全資産価格を更新（並列処理）"""
        try:
            logger.info(f"⚡ === Starting price update for user {user_id} ===")
            
            with db_manager.get_db() as conn:
                if self.use_postgres:
                    from psycopg2.extras import RealDictCursor
                    c = conn.cursor(cursor_factory=RealDictCursor)
                else:
                    c = conn.cursor()
                
                asset_types_to_update = ['jp_stock', 'us_stock', 'gold', 'crypto', 'investment_trust']
                
                query_placeholder = ', '.join(['%s'] * len(asset_types_to_update)) if self.use_postgres else ', '.join(['?'] * len(asset_types_to_update))
                
                if self.use_postgres:
                    c.execute(f'SELECT id, symbol, asset_type FROM assets WHERE user_id = %s AND asset_type IN ({query_placeholder})',
                             [user_id] + asset_types_to_update)
                else:
                    c.execute(f'SELECT id, symbol, asset_type FROM assets WHERE user_id = ? AND asset_type IN ({query_placeholder})',
                             [user_id] + asset_types_to_update)
                
                all_assets = c.fetchall()
                
                if not all_assets:
                    logger.info(f"ℹ️ No assets to update for user {user_id}")
                    return 0
                
                logger.info(f"📦 Found {len(all_assets)} assets to update")
                
                # 並列処理で価格を取得
                updated_prices = price_service.fetch_prices_parallel(all_assets)
                
                if updated_prices:
                    logger.info(f"💾 Updating {len(updated_prices)} assets in database...")
                    
                    try:
                        if self.use_postgres:
                            # PostgreSQLの場合：個別にUPDATE
                            for price_data in updated_prices:
                                c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s',
                                         (float(price_data['price']), str(price_data.get('name', '')), int(price_data['id'])))
                        else:
                            # SQLiteの場合：executemanyを使用
                            update_data = [(float(p['price']), str(p.get('name', '')), int(p['id'])) for p in updated_prices]
                            c.executemany('UPDATE assets SET price = ?, name = ? WHERE id = ?', update_data)
                        
                        # ✅ 明示的にコミット
                        conn.commit()
                        logger.info(f"✅ Database update committed")
                        
                    except Exception as update_error:
                        logger.error(f"❌ Error updating database: {update_error}", exc_info=True)
                        conn.rollback()
                        raise
                
                logger.info(f"✅ === Price update completed: {len(updated_prices)}/{len(all_assets)} assets updated ===")
                return len(updated_prices)
        
        except Exception as e:
            logger.error(f"❌ Error updating prices for user {user_id}: {e}", exc_info=True)
            return 0

# グローバルサービスインスタンス
asset_service = AssetService()
