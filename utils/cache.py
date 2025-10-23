import time
from config import get_config

# ================================================================================
# 💾 キャッシュ機構（メモリ）
# ================================================================================

class SimpleCache:
    """シンプルなメモリキャッシュ"""
    
    def __init__(self, duration=None):
        self.cache = {}
        self.expiry = {}
        self.duration = duration or get_config().CACHE_DURATION
    
    def get(self, key):
        """キャッシュから値を取得"""
        if key in self.cache:
            if time.time() < self.expiry.get(key, 0):
                return self.cache[key]
            else:
                # 期限切れ
                del self.cache[key]
                del self.expiry[key]
        return None
    
    def set(self, key, value):
        """キャッシュに値を保存"""
        self.cache[key] = value
        self.expiry[key] = time.time() + self.duration
    
    def clear(self):
        """キャッシュをクリア"""
        self.cache.clear()
        self.expiry.clear()
    
    def delete(self, key):
        """特定のキーを削除"""
        if key in self.cache:
            del self.cache[key]
            del self.expiry[key]

# グローバルキャッシュインスタンス
price_cache = SimpleCache()