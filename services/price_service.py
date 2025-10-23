import requests
import re
from bs4 import BeautifulSoup
import concurrent.futures
from utils import logger, price_cache, clean_stock_name, extract_number_from_string
from utils.constants import CRYPTO_SYMBOLS, INVESTMENT_TRUST_INFO, INVESTMENT_TRUST_SYMBOLS
from config import get_config

# ================================================================================
# 📈 価格取得サービス
# ================================================================================

# グローバルセッション（接続の再利用）
_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

class PriceService:
    """価格取得を管理"""
    
    def __init__(self, config=None):
        self.config = config or get_config()
        self.timeout = self.config.API_TIMEOUT
    
    def get_jp_stock_info(self, code):
        """日本株情報を取得"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}.T"
            response = _session.get(api_url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    price = 0
                    if 'meta' in result:
                        meta = result['meta']
                        price = (meta.get('regularMarketPrice') or 
                                meta.get('previousClose') or 
                                meta.get('chartPreviousClose') or 0)
                    
                    name = ""
                    if 'meta' in result:
                        meta = result['meta']
                        name = meta.get('shortName') or meta.get('longName') or f"Stock {code}"
                        name = clean_stock_name(name)
                    
                    if price > 0:
                        return {'name': name, 'price': round(float(price), 2)}
        
        except Exception as e:
            logger.error(f"Error getting JP stock {code}: {e}")
        
        return {'name': f'Stock {code}', 'price': 0}
    
    def get_us_stock_info(self, symbol):
        """米国株情報を取得"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            response = _session.get(api_url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    price = 0
                    if 'meta' in result:
                        meta = result['meta']
                        price = (meta.get('regularMarketPrice') or 
                                meta.get('previousClose') or 
                                meta.get('chartPreviousClose') or 0)
                    
                    name = symbol.upper()
                    if 'meta' in result:
                        meta = result['meta']
                        name = meta.get('shortName') or meta.get('longName') or symbol.upper()
                    
                    if price > 0:
                        return {'name': name, 'price': round(float(price), 2)}
        
        except Exception as e:
            logger.error(f"Error getting US stock {symbol}: {e}")
        
        return {'name': symbol.upper(), 'price': 0}
    
    def get_crypto_price(self, symbol):
        """暗号資産価格を取得"""
        cache_key = f"crypto_{symbol}"
        cached = price_cache.get(cache_key)
        if cached:
            return cached

        try:
            symbol = (symbol or '').upper()
            if symbol not in CRYPTO_SYMBOLS:
                logger.warning(f"Unsupported crypto symbol requested: {symbol}")
                return 0.0

            url = f"https://cc.minkabu.jp/pair/{symbol}_JPY"
            response = _session.get(url, timeout=self.timeout)
            response.encoding = response.apparent_encoding
            text = response.text
            
            # JSON形式の価格を探す
            json_matches = re.findall(r'"(?:last|price|lastPrice|close|current|ltp)"\s*:\s*"?([0-9\.,Ee+\-]+)"?', text)
            if json_matches:
                for jm in json_matches:
                    val = extract_number_from_string(jm)
                    if val is not None and val > 0:
                        price_cache.set(cache_key, round(val, 2))
                        return round(val, 2)

            # 「現在値」を探す
            idx = text.find('現在値')
            if idx != -1:
                snippet = text[idx: idx + 700]
                m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*円', snippet)
                if m:
                    try:
                        val = float(m.group(1).replace(',', ''))
                        price_cache.set(cache_key, val)
                        return val
                    except:
                        pass

            # CSSセレクタで探す
            soup = BeautifulSoup(text, 'html.parser')
            selectors = ['div.pairPrice', '.pairPrice', '.pair_price', 'div.priceWrap',
                        'span.yen', 'p.price', 'span.price', 'div.price', 'strong', 'b']
            for sel in selectors:
                try:
                    tag = soup.select_one(sel)
                    if tag:
                        txt = tag.get_text(' ', strip=True)
                        val = extract_number_from_string(txt)
                        if val is not None and val > 0:
                            price_cache.set(cache_key, round(val, 2))
                            return round(val, 2)
                except:
                    pass

            return 0.0
        except Exception as e:
            logger.error(f"Error getting crypto price for {symbol}: {e}")
            return 0.0
    
    def get_gold_price(self):
        """金価格を取得"""
        cache_key = "gold_price"
        cached = price_cache.get(cache_key)
        if cached:
            return cached

        try:
            tanaka_url = "https://gold.tanaka.co.jp/commodity/souba/english/index.php"
            res = _session.get(tanaka_url, timeout=self.timeout)
            res.encoding = res.apparent_encoding
            soup = BeautifulSoup(res.text, "html.parser")
            
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) > 1 and tds[0].get_text(strip=True).upper() == "GOLD":
                    price_text = tds[1].get_text(strip=True)
                    price_match = re.search(r"([0-9,]+) yen", price_text)
                    if price_match:
                        val = int(price_match.group(1).replace(",", ""))
                        price_cache.set(cache_key, val)
                        return val
            return 0
        except Exception as e:
            logger.error(f"Error getting gold price: {e}")
            return 0
    
    def get_investment_trust_price(self, symbol):
        """投資信託価格を取得"""
        cache_key = f"it_{symbol}"
        cached = price_cache.get(cache_key)
        if cached:
            return cached

        if symbol not in INVESTMENT_TRUST_SYMBOLS:
            logger.warning(f"Unsupported investment trust symbol: {symbol}")
            return 0.0

        url = INVESTMENT_TRUST_INFO[symbol]

        try:
            response = _session.get(url, timeout=self.timeout)
            response.encoding = response.apparent_encoding
            soup = BeautifulSoup(response.text, 'html.parser')

            th = soup.find('th', string=re.compile(r'\s*基準価額\s*'))
            
            if th:
                td = th.find_next_sibling('td')
                if td:
                    price_text = td.get_text(strip=True)
                    price = extract_number_from_string(price_text)
                    
                    if price is not None:
                        price_cache.set(cache_key, price)
                        return price

            logger.warning(f"Could not find the price for {symbol}")
            return 0.0

        except Exception as e:
            logger.error(f"Error scraping investment trust price for {symbol}: {e}")
            return 0.0
    
    def get_usd_jpy_rate(self):
        """USD/JPY 相場を取得"""
        cache_key = "usd_jpy"
        cached = price_cache.get(cache_key)
        if cached:
            return cached

        try:
            api_url = "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY=X"
            response = _session.get(api_url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    if 'meta' in result and 'regularMarketPrice' in result['meta']:
                        rate = float(result['meta']['regularMarketPrice'])
                        price_cache.set(cache_key, rate)
                        return rate
            
            return 150.0
        except Exception as e:
            logger.error(f"Error getting USD/JPY rate: {e}")
            return 150.0
    
    def fetch_price(self, asset):
        """単一資産の価格を取得（並列処理用）"""
        asset_type, symbol = asset['asset_type'], asset['symbol']
        price = 0
        try:
            if asset_type == 'jp_stock':
                price = self.get_jp_stock_info(symbol)['price']
            elif asset_type == 'us_stock':
                price = self.get_us_stock_info(symbol)['price']
            elif asset_type == 'gold':
                price = self.get_gold_price()
            elif asset_type == 'crypto':
                price = self.get_crypto_price(symbol)
            elif asset_type == 'investment_trust':
                price = self.get_investment_trust_price(symbol)
            
            return (asset['id'], price) if price > 0 else None
        except Exception as e:
            logger.error(f"Error fetching price for {symbol} ({asset_type}): {e}")
            return None
    
    def fetch_prices_parallel(self, assets):
        """複数資産の価格を並列取得"""
        max_workers = self.config.MAX_WORKERS
        updated_prices = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self.fetch_price, assets)
            updated_prices = [res for res in results if res is not None]
        
        return updated_prices

# グローバルサービスインスタンス
price_service = PriceService()