from .auth import auth_bp
from .dashboard import dashboard_bp
from .assets import assets_bp
from .health import health_bp

def register_blueprints(app):
    """全てのBlueprintを登録"""
    # auth_bpを最初に登録（'/'ルートを処理）
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(assets_bp)
    app.register_blueprint(health_bp)

__all__ = ['register_blueprints']
