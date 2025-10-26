from flask import Blueprint

# ================================================================================
# 🏥 ヘルスチェック
# ================================================================================

health_bp = Blueprint('health', __name__)

@health_bp.route('/ping')
def ping():
    """スリープ防止用のエンドポイント"""

    return "pong", 200

def keep_alive():
    """
    アプリケーションがスリープしないように、定期的に自身にリクエストを送る関数。
    10分（600秒）間隔で実行して、15分のタイムアウトを防ぐ。
    """
    app_url = os.environ.get('RENDER_EXTERNAL_URL')
    
    if not app_url:
        logger.info("RENDER_EXTERNAL_URL is not set. Keep-alive thread will not run.")
        return

    ping_url = f"{app_url}/ping"
    
    while True:
        try:
            logger.info("Sending keep-alive ping...")
            requests.get(ping_url, timeout=10)
            logger.info("Keep-alive ping successful.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Keep-alive ping failed: {e}")
        
        # 10分（600秒）間隔で実行
        time.sleep(600)
