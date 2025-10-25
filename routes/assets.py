from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from models import db_manager
from services import price_service
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
            return user
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
    
    # 資産タイプの検証
    asset_type_names = {
        'jp_stock': '日本株',
        'us_stock': '米国株',
        'cash': '現金',
        'gold': '金(Gold)',
        'crypto': '暗号資産',
        'investment_trust': '投資信託',
        'insurance': '保険'
    }
    
    if asset_type not in asset_type_names:
        flash('無効な資産タイプです', 'error')
        return redirect(url_for('dashboard.dashboard'))
    
    asset_type_name = asset_type_names[asset_type]
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            # 該当する資産を取得
            if db_manager.use_postgres:
                c.execute('''SELECT id, symbol, name, quantity, price, avg_cost, 
                            (quantity * price) as current_value,
                            (quantity * avg_cost) as cost_value,
                            ((quantity * price) - (quantity * avg_cost)) as profit
                            FROM assets 
                            WHERE user_id = %s AND asset_type = %s
                            ORDER BY symbol''', (user_id, asset_type))
            else:
                c.execute('''SELECT id, symbol, name, quantity, price, avg_cost, 
                            (quantity * price) as current_value,
                            (quantity * avg_cost) as cost_value,
                            ((quantity * price) - (quantity * avg_cost)) as profit
                            FROM assets 
                            WHERE user_id = ? AND asset_type = ?
                            ORDER BY symbol''', (user_id, asset_type))
            
            assets = c.fetchall()
            
            # ✅ 修正: 辞書型に変換（dict-likeオブジェクト対応）
            assets_list = []
            for asset in assets:
                # RealDictRowやRow objectを辞書に変換
                asset_dict = dict(asset) if hasattr(asset, 'keys') else {
                    'id': asset[0],
                    'symbol': asset[1],
                    'name': asset[2],
                    'quantity': asset[3],
                    'price': asset[4],
                    'avg_cost': asset[5],
                    'current_value': asset[6],
                    'cost_value': asset[7],
                    'profit': asset[8]
                }
                
                # 数値型に変換
                assets_list.append({
                    'id': int(asset_dict['id']),
                    'symbol': str(asset_dict['symbol']),
                    'name': str(asset_dict['name']) if asset_dict['name'] else str(asset_dict['symbol']),
                    'quantity': float(asset_dict['quantity']) if asset_dict['quantity'] is not None else 0.0,
                    'price': float(asset_dict['price']) if asset_dict['price'] is not None else 0.0,
                    'avg_cost': float(asset_dict['avg_cost']) if asset_dict['avg_cost'] is not None else 0.0,
                    'current_value': float(asset_dict['current_value']) if asset_dict['current_value'] is not None else 0.0,
                    'cost_value': float(asset_dict['cost_value']) if asset_dict['cost_value'] is not None else 0.0,
                    'profit': float(asset_dict['profit']) if asset_dict['profit'] is not None else 0.0
                })
            
            # 合計を計算
            total_current_value = sum(a['current_value'] for a in assets_list)
            total_cost_value = sum(a['cost_value'] for a in assets_list)
            total_profit = total_current_value - total_cost_value
            total_profit_rate = (total_profit / total_cost_value * 100) if total_cost_value > 0 else 0.0
            
            logger.info(f"📊 Loaded {len(assets_list)} {asset_type} assets for user {user_name}")
            
            return render_template('manage_assets.html',
                                 asset_type=asset_type,
                                 asset_type_name=asset_type_name,
                                 assets=assets_list,
                                 total_current_value=total_current_value,
                                 total_cost_value=total_cost_value,
                                 total_profit=total_profit,
                                 total_profit_rate=total_profit_rate,
                                 user_name=user_name,
                                 constants=constants)
    
    except Exception as e:
        logger.error(f"❌ Error loading assets for {asset_type}: {e}", exc_info=True)
        flash('資産の読み込み中にエラーが発生しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/add_asset', methods=['POST'])
def add_asset():
    """資産追加"""
    user = get_current_user()
    if not user:
        return jsonify({'success': False, 'message': 'ログインしてください'}), 401
    
    user_id = user['id']
    
    try:
        asset_type = request.form.get('asset_type')
        symbol = request.form.get('symbol', '').strip()
        quantity = float(request.form.get('quantity', 0))
        avg_cost = float(request.form.get('avg_cost', 0))
        
        if not asset_type or not symbol or quantity <= 0:
            return jsonify({'success': False, 'message': '入力内容を確認してください'}), 400
        
        # 価格を取得
        price = 0.0
        name = symbol
        
        if asset_type not in ['cash', 'insurance']:
            try:
                price_data = price_service.fetch_price({
                    'id': 0,  # 新規追加なのでIDは0
                    'asset_type': asset_type,
                    'symbol': symbol
                })
                if price_data:
                    price = price_data.get('price', 0.0)
                    name = price_data.get('name', symbol)
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch price for {symbol}: {e}")
        
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
        
        logger.info(f"✅ Asset added: {symbol} ({asset_type}) for user {user_id}")
        return jsonify({'success': True, 'message': '資産を追加しました'})
    
    except Exception as e:
        logger.error(f"❌ Error adding asset: {e}", exc_info=True)
        return jsonify({'success': False, 'message': '資産の追加に失敗しました'}), 500

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
            
            # ✅ 修正: dict-likeオブジェクトを辞書に変換
            asset_dict = dict(asset) if hasattr(asset, 'keys') else {}
            
            return render_template('edit_asset.html', asset=asset_dict, constants=constants)
    
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
        quantity = float(request.form.get('quantity', 0))
        avg_cost = float(request.form.get('avg_cost', 0))
        
        if quantity <= 0:
            flash('数量を正しく入力してください', 'error')
            return redirect(url_for('assets.edit_asset', asset_id=asset_id))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('''UPDATE assets 
                            SET quantity = %s, avg_cost = %s 
                            WHERE id = %s AND user_id = %s''',
                         (quantity, avg_cost, asset_id, user_id))
            else:
                c.execute('''UPDATE assets 
                            SET quantity = ?, avg_cost = ? 
                            WHERE id = ? AND user_id = ?''',
                         (quantity, avg_cost, asset_id, user_id))
            
            conn.commit()
        
        logger.info(f"✅ Asset updated: ID {asset_id} for user {user_id}")
        flash('資産を更新しました', 'success')
        return redirect(url_for('dashboard.dashboard'))
    
    except Exception as e:
        logger.error(f"❌ Error updating asset {asset_id}: {e}", exc_info=True)
        flash('資産の更新に失敗しました', 'error')
        return redirect(url_for('dashboard.dashboard'))

@assets_bp.route('/delete_asset', methods=['POST'])
def delete_asset():
    """資産削除"""
    user = get_current_user()
    if not user:
        return jsonify({'success': False, 'message': 'ログインしてください'}), 401
    
    user_id = user['id']
    
    try:
        asset_id = int(request.form.get('asset_id'))
        
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('DELETE FROM assets WHERE id = %s AND user_id = %s', (asset_id, user_id))
            else:
                c.execute('DELETE FROM assets WHERE id = ? AND user_id = ?', (asset_id, user_id))
            
            conn.commit()
        
        logger.info(f"✅ Asset deleted: ID {asset_id} for user {user_id}")
        return jsonify({'success': True, 'message': '資産を削除しました'})
    
    except Exception as e:
        logger.error(f"❌ Error deleting asset {asset_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': '資産の削除に失敗しました'}), 500

@assets_bp.route('/update_prices', methods=['POST'])
def update_prices():
    """価格を更新（非同期）"""
    user = get_current_user()
    if not user:
        return jsonify({'success': False, 'message': 'ログインしてください'}), 401
    
    user_id = user['id']
    asset_type = request.form.get('asset_type')
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT * FROM assets WHERE user_id = %s AND asset_type = %s', (user_id, asset_type))
            else:
                c.execute('SELECT * FROM assets WHERE user_id = ? AND asset_type = ?', (user_id, asset_type))
            
            assets = c.fetchall()
        
        if not assets:
            return jsonify({'success': False, 'message': '更新する資産がありません'}), 400
        
        # ✅ 修正: 並列価格取得
        updated_prices = price_service.fetch_prices_parallel(assets)
        
        if not updated_prices:
            return jsonify({'success': False, 'message': '価格の取得に失敗しました'}), 500
        
        # データベース更新
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            for price_data in updated_prices:
                asset_id = price_data['id']
                new_price = price_data['price']
                new_name = price_data.get('name', '')
                
                if db_manager.use_postgres:
                    c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s',
                             (new_price, new_name, asset_id))
                else:
                    c.execute('UPDATE assets SET price = ?, name = ? WHERE id = ?',
                             (new_price, new_name, asset_id))
            
            conn.commit()
        
        logger.info(f"✅ Updated {len(updated_prices)} prices for user {user_id}")
        return jsonify({'success': True, 'message': f'{len(updated_prices)}件の価格を更新しました'})
    
    except Exception as e:
        logger.error(f"❌ Error updating prices for {asset_type}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': '価格の更新に失敗しました'}), 500

@assets_bp.route('/update_all_prices', methods=['POST'])
def update_all_prices():
    """全資産の価格を更新"""
    user = get_current_user()
    if not user:
        flash('ログインしてください', 'error')
        return redirect(url_for('auth.login'))
    
    user_id = user['id']
    
    try:
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT * FROM assets WHERE user_id = %s', (user_id,))
            else:
                c.execute('SELECT * FROM assets WHERE user_id = ?', (user_id,))
            
            assets = c.fetchall()
        
        if not assets:
            flash('更新する資産がありません', 'warning')
            return redirect(url_for('dashboard.dashboard'))
        
        # ✅ 修正: 並列価格取得
        updated_prices = price_service.fetch_prices_parallel(assets)
        
        if not updated_prices:
            flash('価格の取得に失敗しました', 'error')
            return redirect(url_for('dashboard.dashboard'))
        
        # データベース更新
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            for price_data in updated_prices:
                asset_id = price_data['id']
                new_price = price_data['price']
                new_name = price_data.get('name', '')
                
                if db_manager.use_postgres:
                    c.execute('UPDATE assets SET price = %s, name = %s WHERE id = %s',
                             (new_price, new_name, asset_id))
                else:
                    c.execute('UPDATE assets SET price = ?, name = ? WHERE id = ?',
                             (new_price, new_name, asset_id))
            
            conn.commit()
        
        logger.info(f"✅ Updated all prices ({len(updated_prices)} assets) for user {user_id}")
        flash(f'{len(updated_prices)}件の価格を更新しました', 'success')
        return redirect(url_for('dashboard.dashboard'))
    
    except Exception as e:
        logger.error(f"❌ Error updating all prices: {e}", exc_info=True)
        flash('価格の更新に失敗しました', 'error')
        return redirect(url_for('dashboard.dashboard'))
