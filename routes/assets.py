from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from models import db_manager
from services import price_service, asset_service
from utils import ASSET_TYPE_INFO, CRYPTO_SYMBOLS, INVESTMENT_TRUST_SYMBOLS, INSURANCE_TYPES
from config import get_config

# ================================================================================
# 💼 資産管理ルート
# ================================================================================

assets_bp = Blueprint('assets', __name__)
config = get_config()
use_postgres = config.USE_POSTGRES

def get_current_user():
    """セッションからユーザーを取得"""
    if 'user_id' not in session:
        return None
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('SELECT * FROM users WHERE id = %s', (session['user_id'],))
        else:
            c.execute('SELECT * FROM users WHERE id = ?', (session['user_id'],))
        
        user = c.fetchone()
        return user

@assets_bp.route('/assets/<asset_type>')
def manage_assets(asset_type):
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('''SELECT * FROM assets WHERE user_id = %s AND asset_type = %s
                        ORDER BY symbol''', (user['id'], asset_type))
        else:
            c.execute('''SELECT * FROM assets WHERE user_id = ? AND asset_type = ?
                        ORDER BY symbol''', (user['id'], asset_type))
        
        assets = c.fetchall()
    
    info = ASSET_TYPE_INFO.get(asset_type, ASSET_TYPE_INFO['jp_stock'])
    
    return render_template(
        'manage_assets.html', 
        assets=assets, 
        asset_type=asset_type, 
        info=info, 
        crypto_symbols=CRYPTO_SYMBOLS,
        investment_trust_symbols=INVESTMENT_TRUST_SYMBOLS,
        insurance_types=INSURANCE_TYPES
    )

@assets_bp.route('/add_asset', methods=['POST'])
def add_asset():
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    asset_type = request.form['asset_type']
    symbol = request.form['symbol'].strip()
    if asset_type in ['us_stock', 'crypto']:
        symbol = symbol.upper()

    name = request.form.get('name', '').strip()
    quantity = float(request.form.get('quantity', 0))
    avg_cost = float(request.form.get('avg_cost', 0)) if request.form.get('avg_cost') else 0
    
    price = 0
    if asset_type == 'insurance':
        price = float(request.form.get('price', 0)) if request.form.get('price') else 0
    elif asset_type == 'gold':
        price = price_service.get_gold_price()
        if not name: name = "金 (Gold)"
    elif asset_type == 'crypto':
        if symbol not in CRYPTO_SYMBOLS:
            flash('対応していない暗号資産です', 'error')
            return redirect(url_for('assets.manage_assets', asset_type='crypto'))
        price = price_service.get_crypto_price(symbol)
        name = name or symbol
    elif asset_type == 'investment_trust':
        if symbol not in INVESTMENT_TRUST_SYMBOLS:
            flash('対応していない投資信託です', 'error')
            return redirect(url_for('assets.manage_assets', asset_type='investment_trust'))
        price = price_service.get_investment_trust_price(symbol)
        name = name or symbol
    elif asset_type != 'cash':
        is_jp = (asset_type == 'jp_stock')
        try:
            stock_info = price_service.get_jp_stock_info(symbol) if is_jp else price_service.get_us_stock_info(symbol)
            price = stock_info['price']
            if not name: name = stock_info['name']
        except Exception as e:
            flash(f'価格取得に失敗しました: {symbol}', 'error')
            price = 0
            name = name or symbol
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('''SELECT id, quantity, avg_cost FROM assets 
                        WHERE user_id = %s AND asset_type = %s AND symbol = %s''',
                     (user['id'], asset_type, symbol))
        else:
            c.execute('''SELECT id, quantity, avg_cost FROM assets 
                        WHERE user_id = ? AND asset_type = ? AND symbol = ?''',
                     (user['id'], asset_type, symbol))
        
        existing = c.fetchone()
        
        if existing and asset_type not in ['cash', 'insurance']:
            old_quantity = existing['quantity'] or 0
            old_avg_cost = existing['avg_cost'] or 0
            new_total_quantity = old_quantity + quantity
            
            if new_total_quantity > 0 and avg_cost > 0:
                new_avg_cost = ((old_quantity * old_avg_cost) + (quantity * avg_cost)) / new_total_quantity
            else:
                new_avg_cost = old_avg_cost if old_avg_cost > 0 else avg_cost
            
            update_name = name if name else existing.get('name', symbol)

            if use_postgres:
                c.execute('''UPDATE assets SET quantity = %s, price = %s, name = %s, avg_cost = %s
                            WHERE id = %s''', (new_total_quantity, price, update_name, new_avg_cost, existing['id']))
            else:
                c.execute('''UPDATE assets SET quantity = ?, price = ?, name = ?, avg_cost = ?
                            WHERE id = ?''', (new_total_quantity, price, update_name, new_avg_cost, existing['id']))
            
            flash(f'{symbol} を更新しました', 'success')

        elif existing and asset_type == 'insurance':
            if use_postgres:
                c.execute('''UPDATE assets SET quantity = %s, price = %s, avg_cost = %s, name = %s WHERE id = %s''', 
                         (quantity, price, avg_cost, name, existing['id']))
            else:
                c.execute('''UPDATE assets SET quantity = ?, price = ?, avg_cost = ?, name = ? WHERE id = ?''', 
                         (quantity, price, avg_cost, name, existing['id']))
            flash(f'{symbol} を更新しました', 'success')
        elif existing and asset_type == 'cash':
            if use_postgres:
                c.execute('''UPDATE assets SET price = %s, avg_cost = %s, name = %s WHERE id = %s''', 
                         (price, avg_cost, name or symbol, existing['id']))
            else:
                c.execute('''UPDATE assets SET price = ?, avg_cost = ?, name = ? WHERE id = ?''', 
                         (price, avg_cost, name or symbol, existing['id']))
            flash(f'{symbol} を更新しました', 'success')
        else:
            if use_postgres:
                c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                         (user['id'], asset_type, symbol, name, quantity, price, avg_cost))
            else:
                c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                            VALUES (?, ?, ?, ?, ?, ?, ?)''',
                         (user['id'], asset_type, symbol, name, quantity, price, avg_cost))
            flash(f'{symbol} を追加しました', 'success')
        
        conn.commit()
    
    asset_service.record_asset_snapshot(user['id'])
    
    return redirect(url_for('assets.manage_assets', asset_type=asset_type))

@assets_bp.route('/edit_asset/<int:asset_id>')
def edit_asset(asset_id):
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('SELECT * FROM assets WHERE id = %s AND user_id = %s', (asset_id, user['id']))
        else:
            c.execute('SELECT * FROM assets WHERE id = ? AND user_id = ?', (asset_id, user['id']))
        
        asset = c.fetchone()
    
    if not asset:
        flash('資産が見つかりません', 'error')
        return redirect(url_for('dashboard.dashboard'))
    
    info = ASSET_TYPE_INFO.get(asset['asset_type'], ASSET_TYPE_INFO['jp_stock'])
    
    return render_template('edit_asset.html', asset=asset, info=info, insurance_types=INSURANCE_TYPES)

@assets_bp.route('/update_asset', methods=['POST'])
def update_asset():
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    asset_id = request.form['asset_id']
    symbol = request.form['symbol'].strip()
    name = request.form.get('name', '').strip()
    quantity = float(request.form.get('quantity', 0))
    avg_cost = float(request.form.get('avg_cost', 0)) if request.form.get('avg_cost') else 0
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('SELECT asset_type FROM assets WHERE id = %s AND user_id = %s',
                     (asset_id, user['id']))
        else:
            c.execute('SELECT asset_type FROM assets WHERE id = ? AND user_id = ?',
                     (asset_id, user['id']))
        
        asset = c.fetchone()
        
        if not asset:
            flash('資産が見つかりません', 'error')
            return redirect(url_for('dashboard.dashboard'))
        
        asset_type = asset['asset_type']
        if asset_type in ['us_stock', 'crypto']:
            symbol = symbol.upper()

        price = 0
        if asset_type == 'insurance':
            price = float(request.form.get('price', 0)) if request.form.get('price') else 0
        elif asset_type == 'gold':
            price = price_service.get_gold_price()
            if not name: name = "金 (Gold)"
        elif asset_type == 'crypto':
            if symbol not in CRYPTO_SYMBOLS:
                flash('対応していない暗号資産です', 'error')
                return redirect(url_for('assets.manage_assets', asset_type='crypto'))
            price = price_service.get_crypto_price(symbol)
            if not name: name = symbol
        elif asset_type == 'investment_trust':
            if symbol not in INVESTMENT_TRUST_SYMBOLS:
                flash('対応していない投資信託です', 'error')
                return redirect(url_for('assets.manage_assets', asset_type='investment_trust'))
            price = price_service.get_investment_trust_price(symbol)
            if not name: name = symbol
        elif asset_type != 'cash':
            is_jp = (asset_type == 'jp_stock')
            try:
                stock_info = price_service.get_jp_stock_info(symbol) if is_jp else price_service.get_us_stock_info(symbol)
                price = stock_info['price']
                if not name: name = stock_info['name']
            except Exception as e:
                flash(f'価格取得に失敗しました: {symbol}', 'error')
                price = 0
                name = name or symbol
        
        if use_postgres:
            c.execute('''UPDATE assets SET symbol = %s, name = %s, quantity = %s, price = %s, avg_cost = %s
                        WHERE id = %s AND user_id = %s''',
                     (symbol, name, quantity, price, avg_cost, asset_id, user['id']))
        else:
            c.execute('''UPDATE assets SET symbol = ?, name = ?, quantity = ?, price = ?, avg_cost = ?
                        WHERE id = ? AND user_id = ?''',
                     (symbol, name, quantity, price, avg_cost, asset_id, user['id']))
        
        conn.commit()
    
    asset_service.record_asset_snapshot(user['id'])
    
    flash(f'{symbol} を更新しました', 'success')
    return redirect(url_for('assets.manage_assets', asset_type=asset_type))

@assets_bp.route('/delete_asset', methods=['POST'])
def delete_asset():
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))
    
    asset_id = request.form['asset_id']
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('SELECT asset_type, symbol FROM assets WHERE id = %s AND user_id = %s',
                     (asset_id, user['id']))
        else:
            c.execute('SELECT asset_type, symbol FROM assets WHERE id = ? AND user_id = ?',
                     (asset_id, user['id']))
        
        asset = c.fetchone()
        
        if asset:
            if use_postgres:
                c.execute('DELETE FROM assets WHERE id = %s AND user_id = %s', (asset_id, user['id']))
            else:
                c.execute('DELETE FROM assets WHERE id = ? AND user_id = ?', (asset_id, user['id']))
            
            conn.commit()
            flash(f'{asset["symbol"]} を削除しました', 'success')
            asset_type = asset['asset_type']
        else:
            flash('削除に失敗しました', 'error')
            asset_type = 'jp_stock'
    
    asset_service.record_asset_snapshot(user['id'])
    
    return redirect(url_for('assets.manage_assets', asset_type=asset_type))

@assets_bp.route('/update_prices', methods=['POST'])
def update_prices():
    """非同期価格更新（並列処理版）"""
    user = get_current_user()
    if not user:
        return ('Unauthorized', 401)
    
    asset_type = request.form.get('asset_type')
    if not asset_type:
        return ('Bad Request', 400)

    if asset_type in ['cash', 'insurance']:
        return 'OK'
    
    with db_manager.get_db() as conn:
        # ✅ 修正: db_manager.get_cursor()を使用
        c = db_manager.get_cursor(conn)
        
        if use_postgres:
            c.execute('SELECT id, symbol, asset_type FROM assets WHERE user_id = %s AND asset_type = %s',
                     (user['id'], asset_type))
        else:
            c.execute('SELECT id, symbol, asset_type FROM assets WHERE user_id = ? AND asset_type = ?',
                     (user['id'], asset_type))
        
        assets_to_update = c.fetchall()
    
    if not assets_to_update:
        return 'OK'
    
    # 並列処理で価格を取得
    price_updates = price_service.fetch_prices_parallel(assets_to_update)
    
    if price_updates:
        with db_manager.get_db() as conn:
            c = db_manager.get_cursor(conn)
            if use_postgres:
                from psycopg2.extras import execute_values
                update_query = "UPDATE assets SET price = data.price FROM (VALUES %s) AS data(price, id) WHERE assets.id = data.id"
                execute_values(c, update_query, price_updates)
            else:
                c.executemany('UPDATE assets SET price = ? WHERE id = ?', price_updates)
            conn.commit()
    
    return 'OK'

@assets_bp.route('/update_all_prices', methods=['POST'])
def update_all_prices():
    """全資産の価格更新"""
    user = get_current_user()
    if not user:
        return redirect(url_for('auth.login'))

    updated_count = asset_service.update_user_prices(user['id'])
    asset_service.record_asset_snapshot(user['id'])
    
    flash(f'✅ 資産価格を更新しました({updated_count}件成功)', 'success')
    return redirect(url_for('dashboard.dashboard'))
