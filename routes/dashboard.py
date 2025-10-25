from flask import Blueprint, render_template, session, redirect, url_for, flash
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

@dashboard_bp.route('/dashboard')
def dashboard():
    """ダッシュボード"""
    user_id = session.get('user_id')
    
    # ✅ 修正: ログインチェックを最初に
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

def get_dashboard_data(user_id):
    """ダッシュボード用データを取得"""
    # ... 既存のコードをそのまま維持 ...
    # (長いので省略していますが、変更は不要です)
