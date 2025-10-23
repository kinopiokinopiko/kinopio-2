# ================================================================================
# 📋 定数定義
# ================================================================================

# 暗号通貨銘柄
CRYPTO_SYMBOLS = ['BTC', 'ETH', 'XRP', 'DOGE']

# 投資信託銘柄
INVESTMENT_TRUST_INFO = {
    'S&P500': 'https://www.rakuten-sec.co.jp/web/fund/detail/?ID=JP90C000GKC6',
    'オルカン': 'https://www.rakuten-sec.co.jp/web/fund/detail/?ID=JP90C000H1T1',
    'FANG+': 'https://www.rakuten-sec.co.jp/web/fund/detail/?ID=JP90C000FZD4'
}
INVESTMENT_TRUST_SYMBOLS = list(INVESTMENT_TRUST_INFO.keys())

# 保険種類
INSURANCE_TYPES = ['生命保険', '医療保険', '学資保険', '個人年金保険', 'がん保険', 'その他']

# 資産タイプ
ASSET_TYPES = ['jp_stock', 'us_stock', 'cash', 'gold', 'crypto', 'investment_trust', 'insurance']

# 資産タイプの日本名
ASSET_TYPE_LABELS = {
    'jp_stock': '日本株',
    'us_stock': '米国株',
    'cash': '現金',
    'gold': '金 (Gold)',
    'crypto': '暗号資産',
    'investment_trust': '投資信託',
    'insurance': '保険'
}

# 資産タイプの詳細情報
ASSET_TYPE_INFO = {
    'jp_stock': {
        'title': '日本株',
        'symbol_label': '証券コード',
        'quantity_label': '株数'
    },
    'us_stock': {
        'title': '米国株',
        'symbol_label': 'シンボル',
        'quantity_label': '株数'
    },
    'gold': {
        'title': '金 (Gold)',
        'symbol_label': '種類',
        'quantity_label': '重量(g)'
    },
    'cash': {
        'title': '現金',
        'symbol_label': '項目名',
        'quantity_label': '金額'
    },
    'crypto': {
        'title': '暗号資産',
        'symbol_label': '銘柄',
        'quantity_label': '数量'
    },
    'investment_trust': {
        'title': '投資信託',
        'symbol_label': '銘柄',
        'quantity_label': '保有数量(口)'
    },
    'insurance': {
        'title': '保険',
        'symbol_label': '項目名',
        'quantity_label': '保険金額'
    }
}