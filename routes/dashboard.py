from flask import Blueprint, render_template, session, redirect, url_for, flash
from datetime import datetime, timezone, timedelta
from models import db_manager
from utils import logger
import json

dashboard_bp = Blueprint('dashboard', __name__)

def safe_get(obj, key, default=0.0):
    """辞書またはRow オブジェクトから安全に値を取得"""
    try:
        if obj is None:
            return default
        if hasattr(obj, '__getitem__'):
            val = obj[key]
            return float(val) if val is not None else default
        return default
    except (KeyError, IndexError, TypeError, ValueError) as e:
        logger.debug(f"safe_get error for key '{key}': {e}")
        return default

def get_dashboard_data(user_id):
    """ダッシュボード用データを取得"""
    try:
        logger.info(f"📊 === Starting get_dashboard_data for user_id={user_id} ===")
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            # ✅ 全資産を取得
            if db_manager.use_postgres:
                c.execute('SELECT * FROM assets WHERE user_id = %s ORDER BY asset_type, symbol', (user_id,))
            else:
                c.execute('SELECT * FROM assets WHERE user_id = ? ORDER BY asset_type, symbol', (user_id,))
            
            all_assets = c.fetchall()
            logger.info(f"📦 Fetched {len(all_assets) if all_assets else 0} assets from database")
            
            # 資産タイプごとに分類
            assets_by_type = {
                'jp_stock': [],
                'us_stock': [],
                'cash': [],
                'gold': [],
                'crypto': [],
                'investment_trust': [],
                'insurance': []
            }
            
            if all_assets:
                for asset in all_assets:
                    try:
                        asset_type = asset['asset_type']
                        assets_by_type[asset_type].append(dict(asset))
                        logger.debug(f"  Added asset: {asset['symbol']} ({asset_type})")
                    except (KeyError, TypeError) as e:
                        logger.error(f"❌ Error processing asset: {e}, asset={asset}")
                        continue
            
            logger.info(f"📊 Assets by type: {[(k, len(v)) for k, v in assets_by_type.items() if v]}")
            
            # ✅ 直近2日分の履歴データを取得（prev_*カラムなし）
            if db_manager.use_postgres:
                c.execute('''SELECT record_date, 
                                   jp_stock_value, us_stock_value, cash_value, 
                                   gold_value, crypto_value, investment_trust_value, 
                                   insurance_value, total_value
                            FROM asset_history 
                            WHERE user_id = %s 
                            ORDER BY record_date DESC 
                            LIMIT 2''', (user_id,))
            else:
                c.execute('''SELECT record_date, 
                                   jp_stock_value, us_stock_value, cash_value, 
                                   gold_value, crypto_value, investment_trust_value, 
                                   insurance_value, total_value
                            FROM asset_history 
                            WHERE user_id = ? 
                            ORDER BY record_date DESC 
                            LIMIT 2''', (user_id,))
            
            recent_records = c.fetchall()
            logger.info(f"📅 Fetched {len(recent_records) if recent_records else 0} recent history records")
            
            # 今日と昨日のデータを取得
            today_data = None
            yesterday_data = None
            
            if recent_records and len(recent_records) >= 2:
                today_data = recent_records[0]
                yesterday_data = recent_records[1]
                logger.info(f"📊 Today: {today_data['record_date']}, Yesterday: {yesterday_data['record_date']}")
            elif recent_records and len(recent_records) == 1:
                today_data = recent_records[0]
                logger.info(f"📊 Today: {today_data['record_date']}, No yesterday data")
            else:
                logger.warning("⚠️ No history data found")
            
            # USD/JPY レート取得
            try:
                from services.price_service import price_service
                usd_jpy = price_service.get_usd_jpy_rate()
                logger.info(f"💱 USD/JPY rate: {usd_jpy}")
            except Exception as e:
                logger.warning(f"Failed to get USD/JPY rate: {e}")
                usd_jpy = 150.0
            
            # ✅ 現在の資産値を計算する関数
            def calculate_current_value(assets, asset_type):
                """現在の資産値を計算"""
                total = 0.0
                
                for asset in assets:
                    try:
                        quantity = float(asset.get('quantity', 0))
                        price = float(asset.get('price', 0))
                        
                        if asset_type == 'us_stock':
                            total += quantity * price * usd_jpy
                        elif asset_type == 'investment_trust':
                            total += (quantity * price) / 10000
                        elif asset_type == 'insurance':
                            total += price
                        elif asset_type == 'cash':
                            total += quantity
                        else:
                            total += quantity * price
                    except (TypeError, ValueError) as e:
                        logger.error(f"Error calculating value for asset: {e}")
                        continue
                
                return total
            
            # ✅ 前日比を計算する関数（yesterday_dataを使用）
            def calculate_day_change(current_value, asset_type):
                """前日比を計算（直近2日のデータを比較）"""
                if not yesterday_data:
                    logger.debug(f"  No yesterday_data for {asset_type}, returning 0")
                    return 0.0, 0.0
                
                field_map = {
                    'jp_stock': 'jp_stock_value',
                    'us_stock': 'us_stock_value',
                    'cash': 'cash_value',
                    'gold': 'gold_value',
                    'crypto': 'crypto_value',
                    'investment_trust': 'investment_trust_value',
                    'insurance': 'insurance_value'
                }
                
                field_name = field_map.get(asset_type)
                if not field_name:
                    logger.debug(f"  No field mapping for {asset_type}")
                    return 0.0, 0.0
                
                # ✅ yesterday_dataから前日の値を取得
                yesterday_value = safe_get(yesterday_data, field_name, 0.0)
                day_change = current_value - yesterday_value
                day_change_rate = (day_change / yesterday_value * 100) if yesterday_value > 0 else 0.0
                
                logger.debug(f"  {asset_type}: current=¥{current_value:,.0f}, yesterday=¥{yesterday_value:,.0f}, change=¥{day_change:,.0f} ({day_change_rate:+.2f}%)")
                
                return day_change, day_change_rate
            
            # 計算ロジック
            def get_asset_totals(assets, asset_type):
                """資産の合計・損益・前日比を計算"""
                if not assets:
                    return {
                        'total': 0.0, 'cost': 0.0, 'profit': 0.0, 'profit_rate': 0.0,
                        'day_change': 0.0, 'day_change_rate': 0.0
                    }
                
                try:
                    # 現在の資産値を計算
                    total_value = calculate_current_value(assets, asset_type)
                    
                    # コスト計算
                    cost_value = 0.0
                    for asset in assets:
                        try:
                            quantity = float(asset.get('quantity', 0))
                            avg_cost = float(asset.get('avg_cost', 0))
                            
                            if asset_type == 'us_stock':
                                cost_value += quantity * avg_cost * usd_jpy
                            elif asset_type == 'investment_trust':
                                cost_value += (quantity * avg_cost) / 10000
                            elif asset_type == 'insurance':
                                cost_value += avg_cost
                            elif asset_type == 'cash':
                                cost_value += 0
                            else:
                                cost_value += quantity * avg_cost
                        except (TypeError, ValueError) as e:
                            logger.error(f"Error calculating cost: {e}")
                            continue
                    
                    # 損益計算
                    profit = total_value - cost_value
                    profit_rate = (profit / cost_value * 100) if cost_value > 0 else 0.0
                    
                    # 前日比を計算
                    day_change, day_change_rate = calculate_day_change(total_value, asset_type)
                    
                    return {
                        'total': total_value,
                        'cost': cost_value,
                        'profit': profit,
                        'profit_rate': profit_rate,
                        'day_change': day_change,
                        'day_change_rate': day_change_rate
                    }
                except Exception as e:
                    logger.error(f"❌ Error calculating totals for {asset_type}: {e}", exc_info=True)
                    return {
                        'total': 0.0, 'cost': 0.0, 'profit': 0.0, 'profit_rate': 0.0,
                        'day_change': 0.0, 'day_change_rate': 0.0
                    }
            
            logger.info("📊 Calculating asset totals...")
            
            # 各資産タイプの計算
            jp_stats = get_asset_totals(assets_by_type['jp_stock'], 'jp_stock')
            us_stats = get_asset_totals(assets_by_type['us_stock'], 'us_stock')
            cash_stats = get_asset_totals(assets_by_type['cash'], 'cash')
            gold_stats = get_asset_totals(assets_by_type['gold'], 'gold')
            crypto_stats = get_asset_totals(assets_by_type['crypto'], 'crypto')
            investment_trust_stats = get_asset_totals(assets_by_type['investment_trust'], 'investment_trust')
            insurance_stats = get_asset_totals(assets_by_type['insurance'], 'insurance')
            
            # 総資産（現金を含む）
            total_assets = (jp_stats['total'] + us_stats['total'] + cash_stats['total'] + 
                           gold_stats['total'] + crypto_stats['total'] + 
                           investment_trust_stats['total'] + insurance_stats['total'])
            
            # 損益計算（現金を除外）
            total_cost_excluding_cash = (jp_stats['cost'] + us_stats['cost'] + 
                                         gold_stats['cost'] + crypto_stats['cost'] + 
                                         investment_trust_stats['cost'] + insurance_stats['cost'])
            
            total_value_excluding_cash = (jp_stats['total'] + us_stats['total'] + 
                                          gold_stats['total'] + crypto_stats['total'] + 
                                          investment_trust_stats['total'] + insurance_stats['total'])
            
            total_profit = total_value_excluding_cash - total_cost_excluding_cash
            total_profit_rate = (total_profit / total_cost_excluding_cash * 100) if total_cost_excluding_cash > 0 else 0.0
            
            logger.info(f"💰 Total Assets: ¥{total_assets:,.0f}")
            logger.info(f"📊 Total Profit: ¥{total_profit:,.0f} ({total_profit_rate:+.2f}%)")
            
            # ✅ 総資産の前日比を計算（yesterday_dataのtotal_valueと比較）
            total_day_change = 0.0
            total_day_change_rate = 0.0
            if yesterday_data:
                yesterday_total = safe_get(yesterday_data, 'total_value', 0.0)
                total_day_change = total_assets - yesterday_total
                total_day_change_rate = (total_day_change / yesterday_total * 100) if yesterday_total > 0 else 0.0
                logger.info(f"📈 Total Day Change: current=¥{total_assets:,.0f}, yesterday=¥{yesterday_total:,.0f}, change=¥{total_day_change:,.0f} ({total_day_change_rate:+.2f}%)")
            else:
                logger.warning(f"⚠️ No yesterday_data, total day change = 0")
            
            # チャート用データ
            chart_data = {
                'labels': ['日本株', '米国株', '現金', '金', '暗号資産', '投資信託', '保険'],
                'values': [
                    jp_stats['total'],
                    us_stats['total'],
                    cash_stats['total'],
                    gold_stats['total'],
                    crypto_stats['total'],
                    investment_trust_stats['total'],
                    insurance_stats['total']
                ]
            }
            
            # 履歴データ取得（過去365日）
            if db_manager.use_postgres:
                c.execute('''SELECT record_date, jp_stock_value, us_stock_value, cash_value, 
                                   gold_value, crypto_value, investment_trust_value, 
                                   insurance_value, total_value
                            FROM asset_history 
                            WHERE user_id = %s 
                            ORDER BY record_date ASC 
                            LIMIT 365''', (user_id,))
            else:
                c.execute('''SELECT record_date, jp_stock_value, us_stock_value, cash_value, 
                                   gold_value, crypto_value, investment_trust_value, 
                                   insurance_value, total_value
                            FROM asset_history 
                            WHERE user_id = ? 
                            ORDER BY record_date ASC 
                            LIMIT 365''', (user_id,))
            
            history = c.fetchall() or []
            logger.info(f"📈 Fetched {len(history)} history records")
            
            # 日付文字列に変換
            def format_date(date_obj):
                try:
                    if hasattr(date_obj, 'strftime'):
                        return date_obj.strftime('%m/%d')
                    return str(date_obj)
                except Exception as e:
                    logger.error(f"Error formatting date: {e}")
                    return str(date_obj)
            
            history_data = {
                'dates': [format_date(h['record_date']) for h in history],
                'total': [safe_get(h, 'total_value', 0) for h in history],
                'jp_stock': [safe_get(h, 'jp_stock_value', 0) for h in history],
                'us_stock': [safe_get(h, 'us_stock_value', 0) for h in history],
                'cash': [safe_get(h, 'cash_value', 0) for h in history],
                'gold': [safe_get(h, 'gold_value', 0) for h in history],
                'crypto': [safe_get(h, 'crypto_value', 0) for h in history],
                'investment_trust': [safe_get(h, 'investment_trust_value', 0) for h in history],
                'insurance': [safe_get(h, 'insurance_value', 0) for h in history]
            }
            
            result = {
                'total_assets': total_assets,
                'total_profit': total_profit,
                'total_profit_rate': total_profit_rate,
                'total_day_change': total_day_change,
                'total_day_change_rate': total_day_change_rate,
                'jp_total': jp_stats['total'],
                'jp_profit': jp_stats['profit'],
                'jp_profit_rate': jp_stats['profit_rate'],
                'jp_day_change': jp_stats['day_change'],
                'jp_day_change_rate': jp_stats['day_change_rate'],
                'us_total_jpy': us_stats['total'],
                'us_total_usd': us_stats['total'] / usd_jpy if usd_jpy > 0 else 0.0,
                'us_profit_jpy': us_stats['profit'],
                'us_profit_rate': us_stats['profit_rate'],
                'us_day_change': us_stats['day_change'],
                'us_day_change_rate': us_stats['day_change_rate'],
                'cash_total': cash_stats['total'],
                'gold_total': gold_stats['total'],
                'gold_profit': gold_stats['profit'],
                'gold_profit_rate': gold_stats['profit_rate'],
                'gold_day_change': gold_stats['day_change'],
                'gold_day_change_rate': gold_stats['day_change_rate'],
                'crypto_total': crypto_stats['total'],
                'crypto_profit': crypto_stats['profit'],
                'crypto_profit_rate': crypto_stats['profit_rate'],
                'crypto_day_change': crypto_stats['day_change'],
                'crypto_day_change_rate': crypto_stats['day_change_rate'],
                'investment_trust_total': investment_trust_stats['total'],
                'investment_trust_profit': investment_trust_stats['profit'],
                'investment_trust_profit_rate': investment_trust_stats['profit_rate'],
                'investment_trust_day_change': investment_trust_stats['day_change'],
                'investment_trust_day_change_rate': investment_trust_stats['day_change_rate'],
                'insurance_total': insurance_stats['total'],
                'insurance_profit': insurance_stats['profit'],
                'insurance_profit_rate': insurance_stats['profit_rate'],
                'insurance_day_change': insurance_stats['day_change'],
                'insurance_day_change_rate': insurance_stats['day_change_rate'],
                'jp_stocks': assets_by_type['jp_stock'],
                'us_stocks': assets_by_type['us_stock'],
                'cash_items': assets_by_type['cash'],
                'gold_items': assets_by_type['gold'],
                'crypto_items': assets_by_type['crypto'],
                'investment_trust_items': assets_by_type['investment_trust'],
                'insurance_items': assets_by_type['insurance'],
                'chart_data': json.dumps(chart_data),
                'history_data': json.dumps(history_data)
            }
            
            logger.info(f"✅ === Dashboard data generated successfully ===")
            return result
        
    except Exception as e:
        logger.error(f"❌ Error getting dashboard data: {e}", exc_info=True)
        return None

@dashboard_bp.route('/dashboard')
def dashboard():
    """ダッシュボード"""
    user_id = session.get('user_id')
    
    if not user_id:
        logger.warning("⚠️ Unauthorized access to dashboard, redirecting to login")
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    try:
        logger.info(f"📊 Loading dashboard for user_id: {user_id}")
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT username FROM users WHERE id = %s', (user_id,))
            else:
                c.execute('SELECT username FROM users WHERE id = ?', (user_id,))
            
            user = c.fetchone()
            
            if not user:
                logger.error(f"❌ User not found: {user_id}")
                session.clear()
                flash('ユーザーが見つかりません', 'error')
                return redirect(url_for('auth.login'))
            
            user_name = user['username']
            logger.info(f"✅ User found: {user_name}")
        
        # ダッシュボードデータ取得
        data = get_dashboard_data(user_id)
        
        if data is None:
            logger.warning("⚠️ Dashboard data is None, using default values")
            data = {
                'total_assets': 0, 'total_profit': 0, 'total_profit_rate': 0,
                'total_day_change': 0, 'total_day_change_rate': 0,
                'jp_total': 0, 'jp_profit': 0, 'jp_profit_rate': 0, 'jp_day_change': 0, 'jp_day_change_rate': 0,
                'us_total_jpy': 0, 'us_total_usd': 0, 'us_profit_jpy': 0, 'us_profit_rate': 0, 'us_day_change': 0, 'us_day_change_rate': 0,
                'cash_total': 0, 'gold_total': 0, 'gold_profit': 0, 'gold_profit_rate': 0, 'gold_day_change': 0, 'gold_day_change_rate': 0,
                'crypto_total': 0, 'crypto_profit': 0, 'crypto_profit_rate': 0, 'crypto_day_change': 0, 'crypto_day_change_rate': 0,
                'investment_trust_total': 0, 'investment_trust_profit': 0, 'investment_trust_profit_rate': 0, 'investment_trust_day_change': 0, 'investment_trust_day_change_rate': 0,
                'insurance_total': 0, 'insurance_profit': 0, 'insurance_profit_rate': 0, 'insurance_day_change': 0, 'insurance_day_change_rate': 0,
                'jp_stocks': [], 'us_stocks': [], 'cash_items': [], 'gold_items': [], 'crypto_items': [], 'investment_trust_items': [], 'insurance_items': [],
                'chart_data': json.dumps({'labels': [], 'values': []}),
                'history_data': json.dumps({'dates': [], 'total': [], 'jp_stock': [], 'us_stock': [], 'cash': [], 'gold': [], 'crypto': [], 'investment_trust': [], 'insurance': []})
            }
        
        data['user_name'] = user_name
        data['datetime'] = datetime
        data['timezone'] = timezone
        data['timedelta'] = timedelta
        
        logger.info(f"✅ Rendering dashboard for {user_name}")
        return render_template('dashboard.html', **data)
    
    except Exception as e:
        logger.error(f"❌ Error rendering dashboard: {e}", exc_info=True)
        flash('ダッシュボードの読み込み中にエラーが発生しました', 'error')
        return redirect(url_for('auth.login'))
