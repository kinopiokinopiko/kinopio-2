import logging
import os

# ================================================================================
# 📝 ロギング設定
# ================================================================================

def setup_logger(name, level=logging.INFO):
    """ロガーをセットアップ"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # ハンドラが既にある場合はスキップ
    if logger.hasHandlers():
        return logger
    
    # フォーマッター
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # コンソールハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# グローバルロガーを作成
logger = setup_logger('portfolio_app')