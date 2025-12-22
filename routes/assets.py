from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from models import db_manager
from services import price_service, asset_service
from utils import logger, constants
import json

assets_bp = Blueprint('assets', __name__)

def get_current_user():
    """現在のユーザー情報を取得"""
    user_id = session.get('user_id')
    if not user_id:
        return None
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT id, username FROM users WHERE id = %s', (user_id,))
            else:
                c.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
            
            user = c.fetchone()
            
            if user:
                return {
                    'id': int(user['id']),
                    'username': str(user['username'])
                }
            return None
    except Exception as e:
        logger.error(f"❌ Error getting current user: {e}", exc_info=True)
        return None

@assets_bp.route('/assets/<asset_type>')
def manage_assets(asset_type):
    """資産管理ページ"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    user_name = user['username']
    
    info = constants.ASSET_TYPE_INFO.get(asset_type)
    if not info:
        flash('無効な資産タイプです', 'error')
        return redirect(url_for('dashboard.dashboard'))
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('''SELECT id, symbol, name, quantity, price, avg_cost
                            FROM assets 
                            WHERE user_id = %s AND asset_type = %s
                            ORDER BY symbol''', (user_id, asset_type))
            else:
                c.execute('''SELECT id, symbol, name, quantity, price, avg_cost
                            FROM assets 
                            WHERE user_id = ? AND asset_type = ?
                            ORDER BY symbol''', (user_id, asset_type))
            
            assets = c.fetchall()
            
            assets_list = []
            for asset in assets:
                asset_dict = dict(asset) if hasattr(asset, 'keys') else {
                    'id': asset[0], 'symbol': asset[1], 'name': asset[2],
                    'quantity': asset[3], 'price': asset[4], 'avg_cost': asset[5]
                }
                
                assets_list.append({
                    'id': int(asset_dict['id']),
                    'symbol': str(asset_dict['symbol']),
                    'name': str(asset_dict['name']) if asset_dict['name'] else str(asset_dict['symbol']),
                    'quantity': float(asset_dict['quantity']) if asset_dict['quantity'] is not None else 0.0,
                    'price': float(asset_dict['price']) if asset_dict['price'] is not None else 0.0,
                    'avg_cost': float(asset_dict['avg_cost']) if asset_dict['avg_cost'] is not None else 0.0
                })
            
            logger.info(f"📊 Loaded {len(assets_list)} {asset_type} assets for user {user_name}")
            
            return render_template('manage_assets.html',
                                 asset_type=asset_type,
                                 info=info,
                                 assets=assets_list,
                                 user_name=user_name,
                                 crypto_symbols=constants.CRYPTO_SYMBOLS,
                                 investment_trust_symbols=constants.INVESTMENT_TRUST_SYMBOLS,
                                 insurance_types=constants.INSURANCE_TYPES)
    
    except Exception as e:
        logger.error(f"❌ Error loading assets for {asset_type}: {e}", exc_info=True)
        flash('資産の読み込み中にエラーが発生しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/add_asset', methods=['POST'])
def add_asset():
    """資産追加"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        asset_type = request.form.get('asset_type', '').strip()
        symbol = request.form.get('symbol', '').strip()
        quantity = float(request.form.get('quantity', 0))
        
        if not asset_type or not symbol or quantity <= 0:
            flash('入力内容を確認してください', 'error')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        # 保険の場合
        if asset_type == 'insurance':
            name = request.form.get('name', '').strip()
            avg_cost = float(request.form.get('avg_cost', 0))
            price = float(request.form.get('price', 0))
            
            with db_manager.get_db() as conn:
                c = conn.cursor()
                if db_manager.use_postgres:
                    c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                             (user_id, asset_type, symbol, name, 0, price, avg_cost))
                else:
                    c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                             (user_id, asset_type, symbol, name, 0, price, avg_cost))
                conn.commit()
            
            flash('保険を追加しました', 'success')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        # 現金の場合
        if asset_type == 'cash':
            avg_cost = 0.0
            price = 0.0
            name = symbol
            
            with db_manager.get_db() as conn:
                c = conn.cursor()
                if db_manager.use_postgres:
                    c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                             (user_id, asset_type, symbol, name, quantity, price, avg_cost))
                else:
                    c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                             (user_id, asset_type, symbol, name, quantity, price, avg_cost))
                conn.commit()
            
            flash('現金を追加しました', 'success')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        # その他の資産
        avg_cost = float(request.form.get('avg_cost', 0))
        if avg_cost <= 0:
            flash('平均取得単価を正しく入力してください', 'error')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        price = 0.0
        name = symbol
        
        try:
            price_data = price_service.fetch_price({'id': 0, 'asset_type': asset_type, 'symbol': symbol})
            if price_data and isinstance(price_data, dict):
                price = float(price_data.get('price', 0.0))
                name = str(price_data.get('name', symbol))
        except Exception as e:
            logger.warning(f"⚠️ Error fetching price for {symbol}: {e}")
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                         (user_id, asset_type, symbol, name, quantity, price, avg_cost))
            else:
                c.execute('''INSERT INTO assets (user_id, asset_type, symbol, name, quantity, price, avg_cost)
                            VALUES (?, ?, ?, ?, ?, ?, ?)''',
                         (user_id, asset_type, symbol, name, quantity, price, avg_cost))
            conn.commit()
        
        flash('資産を追加しました', 'success')
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))
    
    except Exception as e:
        logger.error(f"❌ Error adding asset: {e}", exc_info=True)
        flash('資産の追加に失敗しました', 'error')
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))

@assets_bp.route('/edit_asset/<int:asset_id>')
def edit_asset(asset_id):
    """資産編集ページ"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('SELECT * FROM assets WHERE id = %s AND user_id = %s', (asset_id, user_id))
            else:
                c.execute('SELECT * FROM assets WHERE id = ? AND user_id = ?', (asset_id, user_id))
            
            asset = c.fetchone()
            
            if not asset:
                flash('資産が見つかりません', 'error')
                return redirect(url_for('dashboard.dashboard'))
            
            asset_dict = dict(asset) if hasattr(asset, 'keys') else {}
            info = constants.ASSET_TYPE_INFO.get(asset_dict['asset_type'])
            
            return render_template('edit_asset.html', asset=asset_dict, info=info, insurance_types=constants.INSURANCE_TYPES)
    
    except Exception as e:
        logger.error(f"❌ Error loading asset {asset_id}: {e}", exc_info=True)
        flash('資産の読み込み中にエラーが発生しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/update_asset', methods=['POST'])
def update_asset():
    """資産更新"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        asset_id = int(request.form.get('asset_id'))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('SELECT asset_type FROM assets WHERE id = %s AND user_id = %s', (asset_id, user_id))
            else:
                c.execute('SELECT asset_type FROM assets WHERE id = ? AND user_id = ?', (asset_id, user_id))
            
            asset = c.fetchone()
            if not asset:
                flash('資産が見つかりません', 'error')
                return redirect(url_for('dashboard.dashboard'))
            asset_type = asset['asset_type']
        
        if asset_type == 'insurance':
            symbol = request.form.get('symbol', '').strip()
            name = request.form.get('name', '').strip()
            quantity = float(request.form.get('quantity', 0))
            avg_cost = float(request.form.get('avg_cost', 0))
            price = float(request.form.get('price', 0))
            
            with db_manager.get_db() as conn:
                c = conn.cursor()
                if db_manager.use_postgres:
                    c.execute('''UPDATE assets SET symbol = %s, name = %s, quantity = %s, avg_cost = %s, price = %s
                                WHERE id = %s AND user_id = %s''', (symbol, name, quantity, avg_cost, price, asset_id, user_id))
                else:
                    c.execute('''UPDATE assets SET symbol = ?, name = ?, quantity = ?, avg_cost = ?, price = ?
                                WHERE id = ? AND user_id = ?''', (symbol, name, quantity, avg_cost, price, asset_id, user_id))
                conn.commit()
            
            flash('保険を更新しました', 'success')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        quantity = float(request.form.get('quantity', 0))
        avg_cost = float(request.form.get('avg_cost', 0))
        
        if quantity <= 0:
            flash('数量を正しく入力してください', 'error')
            return redirect(url_for('assets.edit_asset', asset_id=asset_id))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('UPDATE assets SET quantity = %s, avg_cost = %s WHERE id = %s AND user_id = %s',
                         (quantity, avg_cost, asset_id, user_id))
            else:
                c.execute('UPDATE assets SET quantity = ?, avg_cost = ? WHERE id = ? AND user_id = ?',
                         (quantity, avg_cost, asset_id, user_id))
            conn.commit()
        
        flash('資産を更新しました', 'success')
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))
    
    except Exception as e:
        logger.error(f"❌ Error updating asset: {e}", exc_info=True)
        flash('資産の更新に失敗しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/delete_asset', methods=['POST'])
def delete_asset():
    """資産削除"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        asset_id = int(request.form.get('asset_id'))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('SELECT asset_type FROM assets WHERE id = %s AND user_id = %s', (asset_id, user_id))
            else:
                c.execute('SELECT asset_type FROM assets WHERE id = ? AND user_id = ?', (asset_id, user_id))
            asset = c.fetchone()
            
            if not asset:
                flash('資産が見つかりません', 'error')
                return redirect(url_for('dashboard.dashboard'))
            asset_type = asset['asset_type']
            
            if db_manager.use_postgres:
                c.execute('DELETE FROM assets WHERE id = %s AND user_id = %s', (asset_id, user_id))
            else:
                c.execute('DELETE FROM assets WHERE id = ? AND user_id = ?', (asset_id, user_id))
            conn.commit()
        
        flash('資産を削除しました', 'success')
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))
    
    except Exception as e:
        logger.error(f"❌ Error deleting asset: {e}", exc_info=True)
        flash('資産の削除に失敗しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/update_prices', methods=['POST'])
def update_prices():
    """特定資産タイプの価格を更新 + スナップショット保存"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    asset_type = request.form.get('asset_type')
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            if db_manager.use_postgres:
                c.execute('SELECT id, asset_type, symbol FROM assets WHERE user_id = %s AND asset_type = %s', 
                         (user_id, asset_type))
            else:
                c.execute('SELECT id, asset_type, symbol FROM assets WHERE user_id = ? AND asset_type = ?', 
                         (user_id, asset_type))
            assets = c.fetchall()
        
        if not assets:
            flash('更新する資産がありません', 'warning')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        assets_list = [{'id': int(a['id']), 'asset_type': str(a['asset_type']), 'symbol': str(a['symbol'])} for a in assets]
        
        updated_prices = price_service.fetch_prices_parallel(assets_list)
        
        if not updated_prices:
            flash('価格の取得に失敗しました', 'error')
            return redirect(url_for('assets.manage_assets', asset_type=asset_type))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            for price_data in updated_prices:
                asset_id = int(price_data['id'])
                new_price = float(price_data['price'])
                new_name = str(price_data.get('name', ''))
                
                if db_manager.use_postgres:
                    c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s', (new_price, new_name, asset_id))
                else:
                    c.execute('UPDATE assets SET price = ?, name = ? WHERE id = ?', (new_price, new_name, asset_id))
            conn.commit()
        
        # ✅ 手動更新後、即座にスナップショットを記録してグラフに反映
        try:
            logger.info(f"📸 Recording snapshot after {asset_type} price update...")
            asset_service.record_asset_snapshot(user_id)
            flash(f'{len(updated_prices)}件の価格を更新し、最新データを保存しました', 'success')
        except Exception as snapshot_error:
            logger.warning(f"⚠️ Failed to record snapshot: {snapshot_error}")
            flash(f'{len(updated_prices)}件の価格を更新しました（スナップショット保存に失敗）', 'success')
        
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))
    
    except Exception as e:
        logger.error(f"❌ Error updating prices: {e}", exc_info=True)
        flash('価格の更新に失敗しました', 'error')
        return redirect(url_for('assets.manage_assets', asset_type=asset_type))

# ✅ 修正: 誤字(@@)を修正し、正しく定義
@assets_bp.route('/update_all_prices', methods=['POST'])
def update_all_prices():
    """全資産の価格を更新 + スナップショット保存"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            asset_types_to_update = ['jp_stock', 'us_stock', 'gold', 'crypto', 'investment_trust']
            ph = ', '.join(['%s'] * len(asset_types_to_update)) if db_manager.use_postgres else ', '.join(['?'] * len(asset_types_to_update))
            
            query = f'SELECT id, asset_type, symbol FROM assets WHERE user_id = {("%s" if db_manager.use_postgres else "?")} AND asset_type IN ({ph})'
            params = [user_id] + asset_types_to_update
            
            c.execute(query, params)
            assets = c.fetchall()
        
        if not assets:
            flash('更新する資産がありません', 'warning')
            return redirect(url_for('dashboard.dashboard'))
        
        assets_list = [{'id': int(a['id']), 'asset_type': str(a['asset_type']), 'symbol': str(a['symbol'])} for a in assets]
        
        logger.info(f"🔄 Starting price update for {len(assets_list)} assets")
        updated_prices = price_service.fetch_prices_parallel(assets_list)
        
        if not updated_prices:
            flash('価格の取得に失敗しました', 'error')
            return redirect(url_for('dashboard.dashboard'))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            for price_data in updated_prices:
                asset_id = int(price_data['id'])
                new_price = float(price_data['price'])
                new_name = str(price_data.get('name', ''))
                
                if db_manager.use_postgres:
                    c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s', (new_price, new_name, asset_id))
                else:
                    c.execute('UPDATE assets SET price = ?, name = ? WHERE id = ?', (new_price, new_name, asset_id))
            conn.commit()
        
        logger.info(f"✅ Updated all prices ({len(updated_prices)} assets) for user {user_id}")
        
        # ✅ 手動更新後、即座にスナップショットを記録してグラフに反映
        try:
            logger.info(f"📸 Recording snapshot after price update for user {user_id}...")
            asset_service.record_asset_snapshot(user_id)
            flash(f'{len(updated_prices)}件の価格を更新し、最新データを保存しました', 'success')
        except Exception as snapshot_error:
            logger.warning(f"⚠️ Failed to record snapshot after price update: {snapshot_error}")
            flash(f'{len(updated_prices)}件の価格を更新しました（スナップショット保存に失敗）', 'success')
        
        return redirect(url_for('dashboard.dashboard'))
    
    except Exception as e:
        logger.error(f"❌ Error updating all prices: {e}", exc_info=True)
        flash('価格の更新に失敗しました', 'error')
        return redirect(url_for('dashboard.dashboard'))
