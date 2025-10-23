from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from models import db_manager
from models.user import UserService
from utils import logger
from config import get_config

# ================================================================================
# 🔐 認証関連
# ================================================================================

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """新規登録"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()
        
        logger.info(f"📝 Registration attempt for user: {username}")
        
        # バリデーション
        if not username or len(username) < 3:
            flash('ユーザー名は3文字以上で入力してください', 'error')
            return redirect(url_for('auth.register'))
        
        if not password or len(password) < 6:
            flash('パスワードは6文字以上で入力してください', 'error')
            return redirect(url_for('auth.register'))
        
        if password != confirm_password:
            flash('パスワードが一致しません', 'error')
            return redirect(url_for('auth.register'))
        
        try:
            config = get_config()
            user_service = UserService(db_manager, config.USE_POSTGRES)
            user_service.create_user(username, password)
            flash('アカウントを作成しました。ログインしてください。', 'success')
            logger.info(f"✅ Registration successful for user: {username}")
            return redirect(url_for('auth.login'))
        except ValueError as e:
            logger.warning(f"⚠️ Registration validation error: {e}")
            flash(str(e), 'error')
            return redirect(url_for('auth.register'))
        except Exception as e:
            logger.error(f"❌ Registration error: {e}", exc_info=True)
            flash('登録に失敗しました', 'error')
            return redirect(url_for('auth.register'))
    
    return render_template('register.html')

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """ログイン"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        
        logger.info(f"🔐 Login attempt for user: {username}")
        
        if not username or not password:
            logger.warning("❌ Login attempt without username or password")
            flash('ユーザー名とパスワードを入力してください', 'error')
            return redirect(url_for('auth.login'))
        
        try:
            config = get_config()
            logger.info(f"📊 Database mode: {'PostgreSQL' if config.USE_POSTGRES else 'SQLite'}")
            
            # ユーザーサービスの初期化
            user_service = UserService(db_manager, config.USE_POSTGRES)
            logger.info(f"✅ UserService initialized")
            
            # パスワード検証
            is_valid = user_service.verify_user(username, password)
            
            if is_valid:
                # ユーザー情報を再取得してセッションに保存
                user = user_service.get_user_by_username(username)
                if user:
                    session['user_id'] = user.id
                    session['username'] = user.username
                    session.permanent = True
                    logger.info(f"✅ Login successful for user: {username} (ID: {user.id})")
                    flash(f'{username}さん、ようこそ！', 'success')
                    return redirect(url_for('dashboard.dashboard'))
                else:
                    logger.error(f"❌ User object is None after verification")
                    flash('ログインに失敗しました', 'error')
                    return redirect(url_for('auth.login'))
            else:
                logger.warning(f"❌ Invalid credentials for user: {username}")
                flash('ユーザー名またはパスワードが正しくありません', 'error')
                return redirect(url_for('auth.login'))
        
        except Exception as e:
            logger.error(f"❌ Login error: {e}", exc_info=True)
            flash('ログインに失敗しました。しばらく時間をおいてから再度お試しください。', 'error')
            return redirect(url_for('auth.login'))
    
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    """ログアウト"""
    username = session.get('username', 'Unknown')
    session.clear()
    logger.info(f"✅ Logout: {username}")
    flash('ログアウトしました', 'success')
    return redirect(url_for('auth.login'))