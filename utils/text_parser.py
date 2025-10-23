import re
from decimal import Decimal, InvalidOperation

# ================================================================================
# 📄 テキスト解析ユーティリティ
# ================================================================================

def normalize_fullwidth(s):
    """全角数字を半角に正規化"""
    if s is None:
        return s
    _FULLWIDTH_TRANS = {ord(f): ord(t) for f, t in zip('０１２３４５６７８９', '0123456789')}
    _FULLWIDTH_TRANS.update({
        ord('，'): ord(','),
        ord('。'): ord('.'),
        ord('＋'): ord('+'),
        ord('－'): ord('-'),
        ord('　'): ord(' '),
        ord('％'): ord('%')
    })
    return s.translate(_FULLWIDTH_TRANS)

def extract_number_from_string(s):
    """文字列から数値を抽出"""
    if not s:
        return None
    
    try:
        s = normalize_fullwidth(s)
    except Exception:
        pass

    s = s.replace('\xa0', ' ')
    
    # 3桁区切りの数値を探す
    m = re.search(r'([+-]?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?(?:[eE][+-]?\d+)?)', s)
    if not m:
        # シンプルな数値を探す
        m = re.search(r'([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)', s)
    
    if not m:
        return None

    num_str = m.group(1)
    num_str = num_str.replace(',', '').replace(' ', '')

    try:
        d = Decimal(num_str)
        return float(d)
    except (InvalidOperation, ValueError):
        try:
            return float(num_str)
        except Exception:
            return None

def clean_stock_name(name):
    """株式名をクリーンアップ"""
    if not name:
        return name
    
    # 日本語のサフィックスを削除
    jp_suffixes = ['株式会社', '合同会社', '合名会社', '合資会社', '有限会社', '(株)', '(株)']
    for suffix in jp_suffixes:
        name = name.replace(suffix, '')
    
    # 英語のサフィックスを削除
    en_suffixes = [
        ' COMPANY, LIMITED', ' COMPANY LIMITED', ' CO., LTD.', ' CO.,LTD.',
        ' CO., LTD', ' CO.,LTD', ' Co., Ltd.', ' CO.LTD', ' LTD.', ' LTD',
        ' INC.', ' INC', ' CORP.', ' CORP'
    ]
    for suffix in en_suffixes:
        if name.upper().endswith(suffix):
            name = name[:-len(suffix)]
            break
    
    return name.strip()