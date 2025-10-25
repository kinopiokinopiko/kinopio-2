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
            # ✅ 修正: conn.cursor() を直接呼び出す
            c = conn.cursor()
            
            if db_manager.use_postgres:
                c.execute('SELECT id, username FROM users WHERE id = %s', (user_id,))
            else:
                c.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
            
            return c.fetchone()
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
            
            # 辞書型に変換
            assets_list = []
            for asset in assets:
                assets_list.append({
                    'id': asset['id'],
                    'symbol': asset['symbol'],
                    'name': asset['name'],
                    'quantity': float(asset['quantity']) if asset['quantity'] else 0.0,
                    'price': float(asset['price']) if asset['price'] else 0.0,
                    'avg_cost': float(asset['avg_cost']) if asset['avg_cost'] else 0.0,
                    'current_value': float(asset['current_value']) if asset['current_value'] else 0.0,
                    'cost_value': float(asset['cost_value']) if asset['cost_value'] else 0.0,
                    'profit': float(asset['profit']) if asset['profit'] else 0.0
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
        logger.error(f"❌ Error loading assets: {e}", exc_info=True)
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
        
        if asset_type != 'cash':
            try:
                price_data = price_service.fetch_price({
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
            
            return render_template('edit_asset.html', asset=dict(asset), constants=constants)
    
    except Exception as e:
        logger.error(f"❌ Error loading asset: {e}", exc_info=True)
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
        logger.error(f"❌ Error updating asset: {e}", exc_info=True)
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
        logger.error(f"❌ Error deleting asset: {e}", exc_info=True)
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
        
        updated_prices = price_service.fetch_prices_parallel(assets)
        
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
        logger.error(f"❌ Error updating prices: {e}", exc_info=True)
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
        
        updated_prices = price_service.fetch_prices_parallel(assets)
        
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
