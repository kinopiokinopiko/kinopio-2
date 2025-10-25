from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash, generate_password_hash
from models import db_manager
from utils import logger

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/')
def index():
    """ルートページ - ログイン済みならダッシュボードへ"""
    if 'user_id' in session:
        return redirect(url_for('dashboard.dashboard'))
    return redirect(url_for('auth.login'))

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """ログインページ"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        logger.info(f"🔐 Login attempt for user: {username}")
        
        # 入力検証
        if not username or not password:
            logger.warning(f"❌ Empty username or password")
            flash('ユーザー名とパスワードを入力してください', 'error')
            return render_template('login.html')
        
        try:
            with db_manager.get_db() as conn:
                if db_manager.use_postgres:
                    from psycopg2.extras import RealDictCursor
                    c = conn.cursor(cursor_factory=RealDictCursor)
                    logger.info("🔌 Using PostgreSQL for login")
                else:
                    c = conn.cursor()
                    logger.info("🔌 Using SQLite for login")
                
                # ユーザー検索
                if db_manager.use_postgres:
                    c.execute('SELECT id, username, password_hash FROM users WHERE username = %s', (username,))
                else:
                    c.execute('SELECT id, username, password_hash FROM users WHERE username = ?', (username,))
                
                user = c.fetchone()
                
                if user:
                    logger.info(f"✅ User found: {username} (ID: {user['id']})")
                    logger.info(f"🔑 Stored hash preview: {user['password_hash'][:50]}...")
                    
                    # パスワード検証
                    if check_password_hash(user['password_hash'], password):
                        logger.info(f"✅ Password verified for user: {username}")
                        session['user_id'] = user['id']
                        session['username'] = user['username']
                        logger.info(f"✅ Session created for user: {username}")
                        flash(f'{username}さん、ようこそ！', 'success')
                        return redirect(url_for('dashboard.dashboard'))
                    else:
                        logger.warning(f"❌ Invalid password for user: {username}")
                        flash('ユーザー名またはパスワードが間違っています', 'error')
                else:
                    logger.warning(f"❌ User not found: {username}")
                    flash('ユーザー名またはパスワードが間違っています', 'error')
        
        except Exception as e:
            logger.error(f"❌ Login error: {e}", exc_info=True)
            flash('ログイン処理中にエラーが発生しました', 'error')
        
        return render_template('login.html')
    
    # GET リクエスト
    return render_template('login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """ユーザー登録ページ"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        password_confirm = request.form.get('password_confirm', '')
        
        logger.info(f"📝 Registration attempt for user: {username}")
        
        # 入力検証
        if not username or not password or not password_confirm:
            flash('全ての項目を入力してください', 'error')
            return render_template('register.html')
        
        if len(username) < 3:
            flash('ユーザー名は3文字以上で入力してください', 'error')
            return render_template('register.html')
        
        if len(password) < 6:
            flash('パスワードは6文字以上で入力してください', 'error')
            return render_template('register.html')
        
        if password != password_confirm:
            flash('パスワードが一致しません', 'error')
            return render_template('register.html')
        
        try:
            with db_manager.get_db() as conn:
                if db_manager.use_postgres:
                    from psycopg2.extras import RealDictCursor
                    c = conn.cursor(cursor_factory=RealDictCursor)
                else:
                    c = conn.cursor()
                
                # ユーザー名の重複チェック
                if db_manager.use_postgres:
                    c.execute('SELECT id FROM users WHERE username = %s', (username,))
                else:
                    c.execute('SELECT id FROM users WHERE username = ?', (username,))
                
                if c.fetchone():
                    logger.warning(f"❌ Username already exists: {username}")
                    flash('このユーザー名は既に使用されています', 'error')
                    return render_template('register.html')
                
                # パスワードをハッシュ化
                password_hash = generate_password_hash(password)
                logger.info(f"🔐 Generated hash preview: {password_hash[:50]}...")
                
                # ユーザー登録
                if db_manager.use_postgres:
                    c.execute('INSERT INTO users (username, password_hash) VALUES (%s, %s)',
                             (username, password_hash))
                else:
                    c.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)',
                             (username, password_hash))
                
                conn.commit()
                logger.info(f"✅ User registered successfully: {username}")
                flash('ユーザー登録が完了しました。ログインしてください。', 'success')
                return redirect(url_for('auth.login'))
        
        except Exception as e:
            logger.error(f"❌ Registration error: {e}", exc_info=True)
            flash('登録処理中にエラーが発生しました', 'error')
        
        return render_template('register.html')
    
    # GET リクエスト
    return render_template('register.html')

@auth_bp.route('/logout')
def logout():
    """ログアウト"""
    username = session.get('username', 'Unknown')
    session.clear()
    logger.info(f"👋 User logged out: {username}")
    flash('ログアウトしました', 'info')
    return redirect(url_for('auth.login'))
