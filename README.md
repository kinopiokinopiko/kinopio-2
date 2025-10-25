# kinopio-2
kinopio-2/
├── 📄 app.py                           # メインアプリケーション
├── 📄 config.py                        # 設定ファイル
├── 📄 Procfile                         # Render デプロイ設定
├── 📄 render.yaml                      # Render サービス設定
├── 📄 requirements.txt                 # Python依存パッケージ
├── 📄 README.md                        # プロジェクト説明
├── 📄 portfolio.db                     # SQLite データベース（ローカル用）
├── 📄 use.py                           # ユーティリティスクリプト
├── 📄 tempCodeRunnerFile.py            # 一時ファイル
│
├── 📁 __pycache__/                     # Pythonキャッシュ
│
├── 📁 models/                          # データモデル
│   ├── 📄 __init__.py                  # モデル初期化
│   ├── 📄 database.py                  # データベース管理（PostgreSQL/SQLite）
│   └── 📄 user.py                      # ユーザーモデル
│
├── 📁 routes/                          # ルート（エンドポイント）
│   ├── 📄 __init__.py                  # ルート初期化・Blueprint登録
│   ├── 📄 auth.py                      # 認証ルート（ログイン/登録/ログアウト）
│   ├── 📄 dashboard.py                 # ダッシュボードルート
│   ├── 📄 assets.py                    # 資産管理ルート
│   └── 📄 health.py                    # ヘルスチェック（/ping）
│
├── 📁 services/                        # ビジネスロジック
│   ├── 📄 __init__.py                  # サービス初期化
│   ├── 📄 asset_service.py             # 資産管理サービス
│   ├── 📄 price_service.py             # 価格取得サービス
│   └── 📄 scheduler_service.py         # スケジューラー・Keep-Alive
│
├── 📁 utils/                           # ユーティリティ
│   ├── 📄 __init__.py                  # ユーティリティ初期化
│   ├── 📄 cache.py                     # キャッシュ機構
│   ├── 📄 constants.py                 # 定数定義
│   ├── 📄 logger.py                    # ロギング設定
│   └── 📄 text_parser.py               # テキスト解析
│
└── 📁 templates/                       # HTMLテンプレート
    ├── 📄 base.html                    # 基本レイアウト
    ├── 📄 login.html                   # ログイン画面
    ├── 📄 register.html                # 新規登録画面
    ├── 📄 dashboard.html               # ダッシュボード（メイン・150行）
    ├── 📄 manage_assets.html           # 資産管理画面
    ├── 📄 edit_asset.html              # 資産編集画面
    │
    └── 📁 components/                  # ダッシュボードコンポーネント
        ├── 📄 dashboard_header.html         # ヘッダーバー
        ├── 📄 dashboard_summary.html        # 総資産カード
        ├── 📄 dashboard_charts.html         # チャートセクション
        ├── 📄 dashboard_asset_cards.html    # 資産カードグリッド
        └── 📄 dashboard_scripts.html        # JavaScriptコード

        # kinopio-2 - 資産管理システム

**個人資産を一元管理できるWebアプリケーション**

リアルタイム価格取得、損益計算、ポートフォリオ可視化機能を備えた資産管理ダッシュボード。

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-2.3.3-green.svg)](https://flask.palletsprojects.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 目次

- [特徴](#-特徴)
- [対応資産](#-対応資産)
- [技術スタック](#-技術スタック)
- [プロジェクト構成](#-プロジェクト構成)
- [セットアップ](#-セットアップ)
- [使い方](#-使い方)
- [デプロイ](#-デプロイ)
- [開発](#-開発)
- [ライセンス](#-ライセンス)

---

## 🚀 特徴

### 📊 資産管理機能
- **7種類の資産に対応**: 日本株、米国株、現金、金、暗号資産、投資信託、保険
- **リアルタイム価格取得**: Yahoo Finance、みんかぶ、田中貴金属などのAPIから自動取得
- **損益計算**: 取得単価と現在価格から自動計算、パーセンテージ表示
- **前日比表示**: 前日からの変動額・変動率をリアルタイム表示

### 📈 可視化機能
- **ポートフォリオ円グラフ**: 資産配分を視覚的に表示
- **折れ線グラフ**: 資産推移を時系列で確認
- **棒グラフ**: 資産タイプ別の積み上げグラフ
- **インタラクティブ**: Chart.jsによる動的グラフ操作

### ⏰ 自動化機能
- **自動価格更新**: 毎日23:58（JST）に全資産価格を自動更新
- **スナップショット記録**: 日次で資産状況を記録し、履歴として保存
- **Keep-Alive**: Renderの無料プランでスリープを防止

### 🔐 セキュリティ
- **ユーザー認証**: Werkzeugによるパスワードハッシュ化
- **セッション管理**: Flask-Sessionによる安全なセッション管理
- **CSRF対策**: セキュアなフォーム送信

---

## 💼 対応資産

| 資産タイプ | 価格取得元 | 対応銘柄 |
|-----------|-----------|---------|
| 🇯🇵 **日本株** | Yahoo Finance Japan | 任意の証券コード |
| 🇺🇸 **米国株** | Yahoo Finance US | 任意のティッカーシンボル |
| 💰 **現金** | - | 手動入力 |
| 🥇 **金(Gold)** | 田中貴金属工業 | グラム単価 |
| ₿ **暗号資産** | みんかぶ暗号資産 | BTC, ETH, XRP, DOGE |
| 📈 **投資信託** | 楽天証券 | S&P500, オルカン, FANG+ |
| 🛡️ **保険** | - | 手動入力（解約返戻金） |

---

## 🛠 技術スタック

### バックエンド
