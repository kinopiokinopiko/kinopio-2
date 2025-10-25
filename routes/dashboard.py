from flask import Blueprint, render_template, session, redirect, url_for
from datetime import datetime, timezone, timedelta
from models import db_manager
from utils import logger
import json

dashboard_bp = Blueprint('dashboard', __name__)

def safe_get(obj, key, default=0.0):
    """辞書またはRow オブジェクトから安全に値を取得"""
    try:
        if hasattr(obj, '__getitem__'):
            val = obj[key]
            return float(val) if val is not None else default
        return default
    except (KeyError, IndexError, TypeError, ValueError):
        return default

def get_dashboard_data(user_id):
    """ダッシュボード用データを取得"""
    try:
        with db_manager.get_db() as conn:
            if db_manager.use_postgres:
                from psycopg2.extras import RealDictCursor
                c = conn.cursor(cursor_factory=RealDictCursor)
            else:
                c = conn.cursor()
            
            # 全資産を取得
            if db_manager.use_postgres:
                c.execute('SELECT * FROM assets WHERE user_id = %s ORDER BY asset_type, symbol', (user_id,))
            else:
                c.execute('SELECT * FROM assets WHERE user_id = ? ORDER BY asset_type, symbol', (user_id,))
            
            all_assets = c.fetchall()
            
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
                    asset_dict = dict(asset) if hasattr(asset, 'keys') else asset
                    assets_by_type[asset_dict['asset_type']].append(asset_dict)
            
            # ✅ 修正: 直近2日分の履歴データを取得
            jst = timezone(timedelta(hours=9))
            today = datetime.now(jst).date()
            
            if db_manager.use_postgres:
                c.execute('''SELECT * FROM asset_history 
                            WHERE user_id = %s 
                            ORDER BY record_date DESC 
                            LIMIT 2''', (user_id,))
            else:
                c.execute('''SELECT * FROM asset_history 
                            WHERE user_id = ? 
                            ORDER BY record_date DESC 
                            LIMIT 2''', (user_id,))
            
            history_records = c.fetchall()
            
            # 今日と昨日のデータを分離
            today_snapshot = None
            yesterday_snapshot = None
            
            if history_records:
                for record in history_records:
                    record_date = record['record_date']
                    if hasattr(record_date, 'date'):
                        record_date = record_date.date()
                    
                    if str(record_date) == str(today):
                        today_snapshot = record
                    else:
                        yesterday_snapshot = record
            
            logger.info(f"📊 Today: {today}, Today snapshot: {today_snapshot is not None}")
            logger.info(f"📊 Yesterday snapshot: {yesterday_snapshot is not None}")
            
            if yesterday_snapshot:
                logger.info(f"📊 Yesterday total: {safe_get(yesterday_snapshot, 'total_value', 0)}")
            
            # USD/JPY レート取得
            try:
                from services.price_service import price_service
                usd_jpy = price_service.get_usd_jpy_rate()
            except Exception as e:
                logger.warning(f"Failed to get USD/JPY rate: {e}")
                usd_jpy = 150.0
            
            # 計算ロジック
            def get_asset_totals(assets, asset_type):
                total_value = 0.0
                cost_value = 0.0
                
                if not assets:
                    return {
                        'total': 0.0, 'cost': 0.0, 'profit': 0.0, 'profit_rate': 0.0,
                        'day_change': 0.0, 'day_change_rate': 0.0
                    }
                
                try:
                    for asset in assets:
                        quantity = float(asset.get('quantity', 0)) if isinstance(asset, dict) else float(asset['quantity'])
                        price = float(asset.get('price', 0)) if isinstance(asset, dict) else float(asset['price'])
                        avg_cost = float(asset.get('avg_cost', 0)) if isinstance(asset, dict) else float(asset['avg_cost'])
                        
                        if asset_type == 'us_stock':
                            total_value += quantity * price * usd_jpy
                            cost_value += quantity * avg_cost * usd_jpy
                        elif asset_type == 'investment_trust':
                            total_value += (quantity * price) / 10000
                            cost_value += (quantity * avg_cost) / 10000
                        elif asset_type == 'insurance':
                            total_value += price
                            cost_value += avg_cost
                        elif asset_type == 'cash':
                            total_value += quantity
                        else:
                            total_value += quantity * price
                            cost_value += quantity * avg_cost
                    
                    profit = total_value - cost_value
                    profit_rate = (profit / cost_value * 100) if cost_value > 0 else 0.0
                    
                    # ✅ 修正: 昨日のスナップショットから前日の値を取得
                    day_change = 0.0
                    day_change_rate = 0.0
                    
                    if yesterday_snapshot:
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
                        if field_name:
                            prev_val = safe_get(yesterday_snapshot, field_name, 0.0)
                            day_change = total_value - prev_val
                            day_change_rate = (day_change / prev_val * 100) if prev_val > 0 else 0.0
                            
                            logger.info(f"📊 {asset_type}: current={total_value:.2f}, prev={prev_val:.2f}, change={day_change:.2f} ({day_change_rate:.2f}%)")
                    
                    return {
                        'total': total_value,
                        'cost': cost_value,
                        'profit': profit,
                        'profit_rate': profit_rate,
                        'day_change': day_change,
                        'day_change_rate': day_change_rate
                    }
                except Exception as e:
                    logger.error(f"Error calculating totals for {asset_type}: {e}")
                    return {
                        'total': 0.0, 'cost': 0.0, 'profit': 0.0, 'profit_rate': 0.0,
                        'day_change': 0.0, 'day_change_rate': 0.0
                    }
            
            # 各資産タイプの計算
            jp_stats = get_asset_totals(assets_by_type['jp_stock'], 'jp_stock')
            us_stats = get_asset_totals(assets_by_type['us_stock'], 'us_stock')
            cash_stats = get_asset_totals(assets_by_type['cash'], 'cash')
            gold_stats = get_asset_totals(assets_by_type['gold'], 'gold')
            crypto_stats = get_asset_totals(assets_by_type['crypto'], 'crypto')
            investment_trust_stats = get_asset_totals(assets_by_type['investment_trust'], 'investment_trust')
            insurance_stats = get_asset_totals(assets_by_type['insurance'], 'insurance')
            
            # 総資産
            total_assets = (jp_stats['total'] + us_stats['total'] + cash_stats['total'] + 
                           gold_stats['total'] + crypto_stats['total'] + 
                           investment_trust_stats['total'] + insurance_stats['total'])
            
            total_cost = (jp_stats['cost'] + us_stats['cost'] + cash_stats['cost'] + 
                         gold_stats['cost'] + crypto_stats['cost'] + 
                         investment_trust_stats['cost'] + insurance_stats['cost'])
            
            total_profit = total_assets - total_cost
            total_profit_rate = (total_profit / total_cost * 100) if total_cost > 0 else 0.0
            
            # ✅ 修正: 総資産の前日比
            total_day_change = 0.0
            total_day_change_rate = 0.0
            if yesterday_snapshot:
                prev_total = safe_get(yesterday_snapshot, 'total_value', 0.0)
                total_day_change = total_assets - prev_total
                total_day_change_rate = (total_day_change / prev_total * 100) if prev_total > 0 else 0.0
                logger.info(f"📊 Total: current={total_assets:.2f}, prev={prev_total:.2f}, change={total_day_change:.2f} ({total_day_change_rate:.2f}%)")
            
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
            
            # 日付文字列に変換
            def format_date(date_obj):
                if hasattr(date_obj, 'strftime'):
                    return date_obj.strftime('%m/%d')
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
            
            return {
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
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}", exc_info=True)
        return None

@dashboard_bp.route('/dashboard')
def dashboard():
    """ダッシュボード"""
    user_id = session.get('user_id')
    
    if not user_id:
        return redirect(url_for('auth.login'))
    
    try:
        with db_manager.get_db() as conn:
            if db_manager.use_postgres:
                from psycopg2.extras import RealDictCursor
                c = conn.cursor(cursor_factory=RealDictCursor)
            else:
                c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT username FROM users WHERE id = %s', (user_id,))
            else:
                c.execute('SELECT username FROM users WHERE id = ?', (user_id,))
            
            user = c.fetchone()
            user_name = user['username'] if user else 'User'
        
        data = get_dashboard_data(user_id)
        
        if data is None:
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
        return render_template('dashboard.html', **data)
    
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}", exc_info=True)
        return redirect(url_for('auth.login'))
