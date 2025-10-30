from datetime import datetime, timezone, timedelta
from utils import logger
from models import db_manager
from config import get_config
from .price_service import price_service

class AssetService:
    """資産管理のビジネスロジック（Neon対応）"""
    
    def __init__(self):
        self.config = get_config()
        self.use_postgres = self.config.USE_POSTGRES
    
    def record_asset_snapshot(self, user_id):
        """現在の資産状況をスナップショットとして記録（Neon対応）"""
        try:
            logger.info(f"📸 === Starting asset snapshot for user {user_id} ===")
            
            # ✅ 短いトランザクションに分割
            # Step 1: 資産データを取得
            assets_by_type = self._fetch_user_assets(user_id)
            
            # Step 2: USD/JPYレートを取得
            usd_jpy = self._get_usd_jpy_rate()
            
            # Step 3: 資産値を計算
            values, total_value = self._calculate_asset_values(assets_by_type, usd_jpy)
            
            # Step 4: 前日のスナップショットを取得
            jst = timezone(timedelta(hours=9))
            today = datetime.now(jst).date()
            yesterday = today - timedelta(days=1)
            
            prev_values, prev_total_value = self._fetch_yesterday_snapshot(user_id, yesterday, values, total_value)
            
            # Step 5: スナップショットを保存（短いトランザクション）
            self._save_snapshot(user_id, today, values, total_value, prev_values, prev_total_value)
            
            logger.info(f"✅ === Asset snapshot completed for user {user_id} on {today} ===")
        
        except Exception as e:
            logger.error(f"❌ Failed to record asset snapshot: {e}", exc_info=True)
            raise
    
    def _fetch_user_assets(self, user_id):
        """ユーザーの資産を取得（短いトランザクション）"""
        try:
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute('SELECT * FROM assets WHERE user_id = %s', (user_id,))
                else:
                    c.execute('SELECT * FROM assets WHERE user_id = ?', (user_id,))
                
                all_assets = c.fetchall()
                
                # 資産タイプごとに分類
                asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
                assets_by_type = {asset_type: [] for asset_type in asset_types}
                
                for asset in all_assets:
                    asset_type = asset['asset_type']
                    if asset_type in assets_by_type:
                        assets_by_type[asset_type].append(dict(asset))
                
                logger.info(f"📦 Fetched assets: {[(k, len(v)) for k, v in assets_by_type.items() if v]}")
                return assets_by_type
        
        except Exception as e:
            logger.error(f"❌ Error fetching user assets: {e}", exc_info=True)
            raise
    
    def _get_usd_jpy_rate(self):
        """USD/JPYレートを取得"""
        try:
            usd_jpy = price_service.get_usd_jpy_rate()
            logger.info(f"💱 USD/JPY rate: {usd_jpy}")
            return usd_jpy
        except Exception as e:
            logger.warning(f"⚠️ Failed to get USD/JPY rate: {e}")
            return 150.0
    
    def _calculate_asset_values(self, assets_by_type, usd_jpy):
        """資産値を計算"""
        values = {}
        asset_types = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']
        
        for asset_type in asset_types:
            assets = assets_by_type.get(asset_type, [])
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
        
        return values, total_value
    
    def _fetch_yesterday_snapshot(self, user_id, yesterday, current_values, current_total):
        """前日のスナップショットを取得（短いトランザクション）"""
        try:
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
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
                    prev_values = current_values.copy()
                    prev_total_value = current_total
                    logger.info(f"⚠️ No yesterday data found, using current values as previous")
                
                return prev_values, prev_total_value
        
        except Exception as e:
            logger.error(f"❌ Error fetching yesterday snapshot: {e}", exc_info=True)
            # エラー時は現在値を返す
            return current_values.copy(), current_total
    
    def _save_snapshot(self, user_id, today, values, total_value, prev_values, prev_total_value):
        """スナップショットを保存（短いトランザクション、Neon対応）"""
        try:
            logger.info(f"💾 Saving snapshot to database...")
            
            # ✅ 短いトランザクションで保存
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    # PostgreSQLの場合：UPSERT（ON CONFLICT）
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
                
                # ✅ 明示的にコミット
                conn.commit()
                logger.info(f"✅ Data committed to database")
            
            # ✅ 別トランザクションで検証
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
                if self.use_postgres:
                    c.execute('SELECT total_value FROM asset_history WHERE user_id = %s AND record_date = %s',
                             (user_id, today))
                else:
                    c.execute('SELECT total_value FROM asset_history WHERE user_id = ? AND record_date = ?',
                             (user_id, today))
                
                saved_record = c.fetchone()
                if saved_record:
                    logger.info(f"✅ Verified: Record saved successfully")
                    logger.info(f"  📊 Saved total: ¥{float(saved_record['total_value'] or 0):,.2f}")
                else:
                    logger.error(f"❌ Verification failed: Record not found after save")
        
        except Exception as save_error:
            logger.error(f"❌ Error saving snapshot: {save_error}", exc_info=True)
            raise
    
    def update_user_prices(self, user_id):
        """特定ユーザーの全資産価格を更新（並列処理、Neon対応）"""
        try:
            logger.info(f"⚡ === Starting price update for user {user_id} ===")
            
            # ✅ Step 1: 価格が必要な資産を取得（短いトランザクション）
            all_assets = self._fetch_assets_to_update(user_id)
            
            if not all_assets:
                logger.info(f"ℹ️ No assets to update for user {user_id}")
                return 0
            
            logger.info(f"📦 Found {len(all_assets)} assets to update")
            
            # ✅ Step 2: 並列処理で価格を取得（DB接続なし）
            updated_prices = price_service.fetch_prices_parallel(all_assets)
            
            if not updated_prices:
                logger.warning(f"⚠️ No prices were updated")
                return 0
            
            # ✅ Step 3: 価格を更新（短いトランザクション）
            updated_count = self._update_asset_prices(updated_prices)
            
            logger.info(f"✅ === Price update completed: {updated_count}/{len(all_assets)} assets updated ===")
            return updated_count
        
        except Exception as e:
            logger.error(f"❌ Error updating prices for user {user_id}: {e}", exc_info=True)
            return 0
    
    def _fetch_assets_to_update(self, user_id):
        """価格更新が必要な資産を取得（短いトランザクション）"""
        try:
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
                
                # 辞書型のリストに変換
                return [dict(asset) for asset in all_assets]
        
        except Exception as e:
            logger.error(f"❌ Error fetching assets to update: {e}", exc_info=True)
            return []
    
    def _update_asset_prices(self, updated_prices):
        """資産価格を更新（短いトランザクション、Neon対応）"""
        try:
            logger.info(f"💾 Updating {len(updated_prices)} assets in database...")
            
            # ✅ 短いトランザクションで更新
            with db_manager.get_db() as conn:
                c = conn.cursor()
                
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
            
            return len(updated_prices)
        
        except Exception as update_error:
            logger.error(f"❌ Error updating database: {update_error}", exc_info=True)
            raise

# グローバルサービスインスタンス
asset_service = AssetService()
