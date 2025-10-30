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
        """現在の資産状況をスナップショットとして記録（前日比を含む）- Neon PostgreSQL対応強化版"""
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                logger.info(f"📸 === Starting asset snapshot for user {user_id} (attempt {attempt + 1}/{max_retries}) ===")
                
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
                    logger.info(f"📅 Yesterday's date: {yesterday}")
                    
                    asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
                    values = {}
                    
                    # USD/JPYレートを取得
                    try:
                        usd_jpy = price_service.get_usd_jpy_rate()
                        logger.info(f"💱 USD/JPY rate: {usd_jpy}")
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
                        logger.info(f"  📊 {asset_type}: ¥{total:,.2f}")
                    
                    total_value = sum(values.values())
                    logger.info(f"  💰 Total: ¥{total_value:,.2f}")
                    
                    # 昨日のスナップショットを取得
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
                    
                    # 前日のデータ処理
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
                    
                    # スナップショット保存
                    try:
                        if self.use_postgres:
                            logger.info(f"💾 Saving to PostgreSQL (Neon)...")
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
                            logger.info(f"💾 Saving to SQLite...")
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
                        
                        # 明示的にコミット
                        conn.commit()
                        logger.info(f"✅ Data committed to database")
                        
                        # 保存確認
                        if self.use_postgres:
                            c.execute('SELECT * FROM asset_history WHERE user_id = %s AND record_date = %s',
                                     (user_id, today))
                        else:
                            c.execute('SELECT * FROM asset_history WHERE user_id = ? AND record_date = ?',
                                     (user_id, today))
                        
                        saved_record = c.fetchone()
                        if saved_record:
                            logger.info(f"✅ Verified: Record saved successfully")
                            logger.info(f"  📊 Saved total: ¥{float(saved_record['total_value'] or 0):,.2f}")
                        else:
                            raise Exception("Verification failed: Record not found after save")
                    
                    except Exception as save_error:
                        logger.error(f"❌ Error saving snapshot: {save_error}", exc_info=True)
                        conn.rollback()
                        raise
                    
                    # 前日比ログ
                    day_changes = {}
                    for asset_type in asset_types:
                        change = values[asset_type] - prev_values[asset_type]
                        change_rate = (change / prev_values[asset_type] * 100) if prev_values[asset_type] > 0 else 0
                        day_changes[asset_type] = (change, change_rate)
                        logger.info(f"  📈 {asset_type}: {'+' if change >= 0 else ''}¥{change:,.2f} ({'+' if change_rate >= 0 else ''}{change_rate:.2f}%)")
                    
                    total_change = total_value - prev_total_value
                    total_change_rate = (total_change / prev_total_value * 100) if prev_total_value > 0 else 0
                    logger.info(f"  💹 Total change: {'+' if total_change >= 0 else ''}¥{total_change:,.2f} ({'+' if total_change_rate >= 0 else ''}{total_change_rate:.2f}%)")
                    
                    logger.info(f"✅ === Asset snapshot completed for user {user_id} on {today} ===")
                    return True
            
            except Exception as e:
                last_error = e
                logger.error(f"❌ Snapshot attempt {attempt + 1}/{max_retries} failed: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"❌ All {max_retries} snapshot attempts failed for user {user_id}")
        raise Exception(f"Failed to record snapshot after {max_retries} attempts: {last_error}")
    
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
                            for price_data in updated_prices:
                                c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s',
                                         (float(price_data['price']), str(price_data.get('name', '')), int(price_data['id'])))
                        else:
                            update_data = [(float(p['price']), str(p.get('name', '')), int(p['id'])) for p in updated_prices]
                            c.executemany('UPDATE assets SET price = ?, name = ? WHERE id = ?', update_data)
                        
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
