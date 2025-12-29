import requests
from bs4 import BeautifulSoup
import time
import random
import concurrent.futures
from utils import logger, cache
import re

class PriceService:
    def __init__(self, config):
        self.config = config
        self.cache = cache.SimpleCache(duration=300)  # 5分キャッシュ
        self.session = requests.Session()
        
        # User-Agentをランダム化
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        self._update_user_agent()
    
    def _update_user_agent(self):
        """User-Agentをランダムに更新"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def fetch_price(self, asset):
        """単一資産の価格を取得"""
        try:
            if hasattr(asset, 'keys'):
                asset_dict = dict(asset)
            elif isinstance(asset, dict):
                asset_dict = asset
            else:
                logger.error(f"❌ Unexpected asset type: {type(asset)}")
                return None
            
            asset_id = asset_dict['id']
            asset_type = asset_dict['asset_type']
            symbol = asset_dict['symbol']
            
            logger.debug(f"🔍 Fetching price for {symbol} ({asset_type})")
            
            if asset_type in ['cash', 'insurance']:
                return None
            
            cache_key = f"{asset_type}:{symbol}"
            cached = self.cache.get(cache_key)
            if cached:
                return {
                    'id': asset_id,
                    'symbol': symbol,
                    'price': cached['price'],
                    'name': cached.get('name', symbol)
                }
            
            time.sleep(random.uniform(0.5, 1.5))
            self._update_user_agent()
            
            price = 0.0
            name = symbol
            
            try:
                if asset_type == 'jp_stock':
                    price, name = self._fetch_jp_stock(symbol)
                elif asset_type == 'us_stock':
                    price, name = self._fetch_us_stock(symbol)
                elif asset_type == 'gold':
                    price, name = self._fetch_gold_price()
                elif asset_type == 'crypto':
                    price, name = self._fetch_crypto(symbol)
                elif asset_type == 'investment_trust':
                    price, name = self._fetch_investment_trust(symbol)
                else:
                    logger.warning(f"⚠️ Unknown asset type: {asset_type}")
                    return None
            
            except Exception as fetch_error:
                logger.warning(f"⚠️ Failed to fetch price for {symbol}, skipping: {fetch_error}")
                return None
            
            self.cache.set(cache_key, {'price': price, 'name': name})
            
            return {
                'id': asset_id,
                'symbol': symbol,
                'price': price,
                'name': name
            }
        
        except Exception as e:
            logger.warning(f"⚠️ Error fetching price for {symbol if 'symbol' in locals() else 'unknown'}: {e}")
            return None
    
    def fetch_prices_parallel(self, assets):
        """複数資産の価格を並列取得"""
        if not assets:
            return []
        
        max_workers = min(5, len(assets))
        updated_prices = []
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_asset = {executor.submit(self.fetch_price, asset): asset for asset in assets}
                
                for future in concurrent.futures.as_completed(future_to_asset, timeout=180):
                    try:
                        result = future.result(timeout=15)
                        if result is not None and isinstance(result, dict):
                            updated_prices.append(result)
                    except Exception:
                        continue
            
            return updated_prices
        except Exception as e:
            logger.error(f"❌ Error in parallel fetch: {e}", exc_info=True)
            return updated_prices
    
    def _fetch_jp_stock(self, symbol):
        """日本株の価格と名称を取得"""
        # 1. 価格取得 (API)
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.T"
            api_res = self.session.get(api_url, timeout=5)
            price = 0.0
            
            if api_res.status_code == 200:
                data = api_res.json()
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    meta = data['chart']['result'][0]['meta']
                    price = (meta.get('regularMarketPrice') or 
                           meta.get('previousClose') or 
                           meta.get('chartPreviousClose') or 0)
        except Exception as e:
            logger.error(f"Error fetching JP stock price: {e}")
            raise

        # 2. 名称取得 (Yahoo!ファイナンス日本版スクレイピング)
        name = f"Stock {symbol}"
        try:
            url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 画像の構造に合わせてH1を取得
                # <header>タグ内、もしくはメインコンテンツ内の最初のh1を探す
                h1 = soup.find('h1')
                if h1:
                    raw_name = h1.get_text(strip=True)
                    # 不要な文言の削除
                    cleanup_patterns = [
                        r'の株価.*', r'【.*】', r'\(株\)', r'（株）', r'株式会社'
                    ]
                    cleaned_name = raw_name
                    for pattern in cleanup_patterns:
                        cleaned_name = re.sub(pattern, '', cleaned_name)
                    
                    if cleaned_name.strip():
                        name = cleaned_name.strip()
                        logger.info(f"✅ Scraped JP Name: {name}")
                    else:
                        # 全部消えてしまった場合は元のテキストを使用
                        name = raw_name
        except Exception as e:
            logger.warning(f"JP stock name scraping failed: {e}")

        if price > 0:
            return price, name
        raise ValueError(f"Price not found for {symbol}")

    def _fetch_us_stock(self, symbol):
        """米国株の価格を取得"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            response = self.session.get(api_url, timeout=10)
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart']:
                result = data['chart']['result'][0]
                meta = result['meta']
                price = (meta.get('regularMarketPrice') or meta.get('previousClose') or 0)
                name = meta.get('shortName') or symbol.upper()
                if price > 0:
                    return round(float(price), 2), name
            raise ValueError("Price not found")
        except Exception as e:
            logger.error(f"Error US stock: {e}")
            raise
    
    def _fetch_gold_price(self):
        """金価格を取得"""
        try:
            url = "https://gold.tanaka.co.jp/commodity/souba/english/index.php"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) > 1 and 'GOLD' in tds[0].get_text(strip=True).upper():
                    price = int(re.search(r'([0-9,]+)', tds[1].get_text()).group(1).replace(',', ''))
                    return price, "金(Gold)"
            raise ValueError("Gold price not found")
        except Exception as e:
            logger.error(f"Error gold: {e}")
            raise
    
    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得 (Yahoo!ファイナンスAPIエンドポイント使用)"""
        try:
            symbol = symbol.upper()
            # Yahoo! Financeのシンボル形式に変換 (BTC -> BTC-JPY)
            yahoo_symbol = f"{symbol}-JPY"
            
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            response = self.session.get(api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    meta = data['chart']['result'][0]['meta']
                    price = (meta.get('regularMarketPrice') or 
                           meta.get('previousClose') or 0)
                    
                    if price > 0:
                        logger.info(f"✅ Crypto ({symbol}): ¥{price:,.0f}")
                        return float(price), symbol
            
            # バックアップ: みんかぶ (既存ロジック)
            url = f"https://cc.minkabu.jp/pair/{symbol}_JPY"
            text = self.session.get(url, timeout=10).text
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*円', text)
            if m:
                val = float(m.group(1).replace(',', ''))
                return val, symbol
                
            raise ValueError(f"Crypto price not found for {symbol}")
        except Exception as e:
            logger.error(f"❌ Error getting crypto {symbol}: {e}")
            raise
    
    def _fetch_investment_trust(self, symbol):
        """投資信託の価格を取得"""
        try:
            symbol_map = {
                'S&P500': 'JP90C000GKC6',
                'オルカン': 'JP90C000H1T1',
                'FANG+': 'JP90C000FZD4'
            }
            if symbol not in symbol_map: raise ValueError(f"Unknown fund: {symbol}")
            
            fund_id = symbol_map[symbol]
            url = f"https://www.rakuten-sec.co.jp/web/fund/detail/?ID={fund_id}"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            th = soup.find('th', string=re.compile(r'基準価額'))
            if th and th.find_next_sibling('td'):
                val_text = th.find_next_sibling('td').get_text()
                val = re.search(r'([0-9,]+)', val_text)
                if val: return float(val.group(1).replace(',', '')), symbol
            
            raise ValueError("Fund price not found")
        except Exception as e:
            logger.error(f"Error fund: {e}")
            raise
    
    def get_usd_jpy_rate(self):
        """USD/JPYレートを取得"""
        try:
            cached = self.cache.get("USD_JPY")
            if cached: return cached['rate']
            
            api_url = "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY=X"
            response = self.session.get(api_url, timeout=10)
            data = response.json()
            rate = data['chart']['result'][0]['meta']['regularMarketPrice']
            
            self.cache.set("USD_JPY", {'rate': rate})
            return rate
        except Exception:
            return 150.0

# グローバルインスタンス
from config import get_config
price_service = PriceService(get_config())
