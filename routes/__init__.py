from flask import redirect, url_for
from utils import logger

def register_blueprints(app):
    """全てのBlueprintを登録"""
    
    logger.info("📦 Registering blueprints...")
    
    try:
        from .auth import auth_bp
        app.register_blueprint(auth_bp)
        logger.info("✅ auth_bp registered")
    except Exception as e:
        logger.error(f"❌ Failed to register auth_bp: {e}", exc_info=True)
        raise
    
    try:
        from .dashboard import dashboard_bp
        app.register_blueprint(dashboard_bp)
        logger.info("✅ dashboard_bp registered")
    except Exception as e:
        logger.error(f"❌ Failed to register dashboard_bp: {e}", exc_info=True)
        raise
    
    try:
        from .assets import assets_bp
        app.register_blueprint(assets_bp)
        logger.info("✅ assets_bp registered")
    except Exception as e:
        logger.error(f"❌ Failed to register assets_bp: {e}", exc_info=True)
        raise
    
    try:
        from .health import health_bp
        app.register_blueprint(health_bp)
        logger.info("✅ health_bp registered")
    except Exception as e:
        logger.error(f"❌ Failed to register health_bp: {e}", exc_info=True)
        raise
    
    logger.info("✅ All blueprints registered successfully")
    
    return app
