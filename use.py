#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
🚀 use.py - アプリケーションエントリーポイント
================================================================================

開発環境と本番環境の両方で動作するメインスクリプト。
このファイルからアプリケーションを起動します。

使用方法:
    開発環境: python use.py
    本番環境: gunicorn use:app (Procfile経由)
    または: flask run
"""

import os
import sys
from dotenv import load_dotenv

# .envファイルを読み込む（開発環境向け）
load_dotenv()

# アプリケーションファクトリをインポート
from app import create_app
from utils import logger

# ================================================================================
# 🚀 アプリケーション作成
# ================================================================================

# 環境に応じたアプリケーションを作成
app = create_app()

# ================================================================================
# 🏃 メイン実行
# ================================================================================

if __name__ == '__main__':
    try:
        # ポート番号を取得（デフォルト: 5000）
        port = int(os.environ.get('PORT', 5000))
        
        # ホストを取得（デフォルト: 0.0.0.0）
        host = os.environ.get('HOST', '0.0.0.0')
        
        # 環境を取得
        env = os.environ.get('FLASK_ENV', 'development')
        debug = env == 'development'
        
        logger.info(f"🌍 Environment: {env}")
        logger.info(f"🌐 Starting Flask app on {host}:{port}")
        logger.info(f"🐛 Debug mode: {debug}")
        
        # アプリケーションを起動
        app.run(
            host=host,
            port=port,
            debug=debug,
            use_reloader=debug
        )
    
    except KeyboardInterrupt:
        logger.info("\n⛔ Application interrupted by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)