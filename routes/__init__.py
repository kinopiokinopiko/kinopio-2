from .auth import auth_bp
from .health import health_bp
from .dashboard import dashboard_bp
from .assets import assets_bp

def register_blueprints(app):
    """すべてのBlueprintをアプリに登録"""
    app.register_blueprint(auth_bp)
    app.register_blueprint(health_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(assets_bp)

__all__ = ['register_blueprints']