from datetime import datetime, timezone, timedelta
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
        """現在の資産状況をスナップショットとして記録（前日比を含む）"""
        try:
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
                jst = timezone(timedelta(hours=9))
                today = datetime.now(jst).date()
                yesterday = today - timedelta(days=1)
                
                asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
                values = {}
                
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
                        usd_jpy = price_service.get_usd_jpy_rate()
                        total = sum(a['quantity'] * a['price'] for a in assets) * usd_jpy
                    elif asset_type == 'investment_trust':
                        total = sum((a['quantity'] * a['price'] / 10000) for a in assets)
                    elif asset_type == 'insurance':
                        total = sum(a['price'] for a in assets)
                    elif asset_type == 'cash':
                        total = sum(a['quantity'] for a in assets)
                    else:
                        total = sum(a['quantity'] * a['price'] for a in assets)
                    
                    values[asset_type] = total
                
                total_value = sum(values.values())
                
                # 前日のスナップショットを取得
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
                
                prev_record = c.fetchone()
                
                # 前日のデータがない場合は0として扱う
                prev_values = {
                    'jp_stock': prev_record['jp_stock_value'] if prev_record else 0,
                    'us_stock': prev_record['us_stock_value'] if prev_record else 0,
                    'cash': prev_record['cash_value'] if prev_record else 0,
                    'gold': prev_record['gold_value'] if prev_record else 0,
                    'crypto': prev_record['crypto_value'] if prev_record else 0,
                    'investment_trust': prev_record['investment_trust_value'] if prev_record else 0,
                    'insurance': prev_record['insurance_value'] if prev_record else 0,
                }
                prev_total_value = prev_record['total_value'] if prev_record else 0
                
                # 当日のスナップショットを保存（前日比を含む）
                if self.use_postgres:
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
                logger.info(f"✅ Asset snapshot recorded for user {user_id}")
        
        except Exception as e:
            logger.error(f"❌ Failed to record asset snapshot: {e}", exc_info=True)
    
    def update_user_prices(self, user_id):
        """特定ユーザーの全資産価格を更新（並列処理）"""
        try:
            logger.info(f"⚡ Starting price update for user {user_id}")
            
            with db_manager.get_db() as conn:
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
                    logger.info(f"No assets to update for user {user_id}")
                    return 0
                
                # 並列処理で価格を取得
                updated_prices = price_service.fetch_prices_parallel(all_assets)
                
                if updated_prices:
                    logger.info(f"💾 Updating {len(updated_prices)} assets...")
                    if self.use_postgres:
                        from psycopg2.extras import execute_values
                        update_query = "UPDATE assets SET price = data.price FROM (VALUES %s) AS data(price, id) WHERE assets.id = data.id"
                        execute_values(c, update_query, updated_prices)
                    else:
                        c.executemany('UPDATE assets SET price = ? WHERE id = ?', updated_prices)
                
                conn.commit()
                logger.info(f"✅ Price update completed: {len(updated_prices)}/{len(all_assets)} assets updated")
                return len(updated_prices)
        
        except Exception as e:
            logger.error(f"❌ Error updating prices for user {user_id}: {e}", exc_info=True)
            return 0

# グローバルサービスインスタンス
asset_service = AssetService()