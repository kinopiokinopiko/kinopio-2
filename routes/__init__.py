from flask import redirect, url_for

def register_blueprints(app):
    """全てのBlueprintを登録"""
    from .auth import auth_bp
    from .dashboard import dashboard_bp
    from .assets import assets_bp
    from .health import health_bp
    
    # Blueprintを登録
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(assets_bp)
    app.register_blueprint(health_bp)
    
    # ✅ ルートパスへのアクセスをログインページにリダイレクト
    @app.route('/')
    def index():
        return redirect(url_for('auth.login'))
    
    return app
