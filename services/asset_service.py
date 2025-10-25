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
                # PostgreSQL/SQLiteの統一インターフェース
                if self.use_postgres:
                    from psycopg2.extras import RealDictCursor
                    c = conn.cursor(cursor_factory=RealDictCursor)
                else:
                    c = conn.cursor()
                
                jst = timezone(timedelta(hours=9))
                today = datetime.now(jst).date()
                yesterday = today - timedelta(days=1)
                
                logger.info(f"📸 Recording asset snapshot for user {user_id}, date: {today}")
                
                asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
                values = {}
                
                # ✅ 修正: USD/JPYレートを取得
                try:
                    usd_jpy = price_service.get_usd_jpy_rate()
                    logger.info(f"💱 USD/JPY rate: {usd_jpy}")
                except Exception as e:
                    logger.warning(f"Failed to get USD/JPY rate: {e}")
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
                    logger.info(f"  {asset_type}: ¥{total:,.2f}")
                
                total_value = sum(values.values())
                logger.info(f"  📊 Total: ¥{total_value:,.2f}")
                
                # ✅ 修正: 昨日のスナップショットを取得（前日の値として使用）
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
                    logger.info(f"📅 Yesterday's data found: Total ¥{prev_total_value:,.2f}")
                else:
                    # 前日のデータがない場合は、今日のデータを前日の値としても使用（初回記録時）
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
                    logger.info(f"⚠️ No yesterday data found, using current values as previous")
                
                # ✅ 修正: 当日のスナップショットを保存または更新
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
                                    total_value = EXCLUDED.total_value''',
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
                
                # ✅ デバッグ: 前日比を計算して表示
                day_changes = {}
                for asset_type in asset_types:
                    change = values[asset_type] - prev_values[asset_type]
                    change_rate = (change / prev_values[asset_type] * 100) if prev_values[asset_type] > 0 else 0
                    day_changes[asset_type] = (change, change_rate)
                    logger.info(f"  📊 {asset_type}: {'+' if change >= 0 else ''}¥{change:,.2f} ({'+' if change_rate >= 0 else ''}{change_rate:.2f}%)")
                
                total_change = total_value - prev_total_value
                total_change_rate = (total_change / prev_total_value * 100) if prev_total_value > 0 else 0
                logger.info(f"  📊 Total change: {'+' if total_change >= 0 else ''}¥{total_change:,.2f} ({'+' if total_change_rate >= 0 else ''}{total_change_rate:.2f}%)")
                
                logger.info(f"✅ Asset snapshot recorded for user {user_id} on {today}")
        
        except Exception as e:
            logger.error(f"❌ Failed to record asset snapshot: {e}", exc_info=True)
    
    def update_user_prices(self, user_id):
        """特定ユーザーの全資産価格を更新（並列処理）"""
        try:
            logger.info(f"⚡ Starting price update for user {user_id}")
            
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
