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
        self.cache = cache.SimpleCache(duration=300)
        self.session = requests.Session()
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self._update_user_agent()
    
    def _update_user_agent(self):
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8'
        })
    
    def fetch_price(self, asset):
        try:
            if hasattr(asset, 'keys'): asset_dict = dict(asset)
            elif isinstance(asset, dict): asset_dict = asset
            else: return None
            
            asset_id = asset_dict['id']
            asset_type = asset_dict['asset_type']
            symbol = asset_dict['symbol']
            
            if asset_type in ['cash', 'insurance']: return None
            
            cache_key = f"{asset_type}:{symbol}"
            cached = self.cache.get(cache_key)
            if cached:
                return {
                    'id': asset_id, 'symbol': symbol,
                    'price': cached['price'], 'name': cached.get('name', symbol)
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
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch price for {symbol}: {e}")
                return None
            
            self.cache.set(cache_key, {'price': price, 'name': name})
            return {'id': asset_id, 'symbol': symbol, 'price': price, 'name': name}
        
        except Exception as e:
            logger.warning(f"⚠️ Error fetching price for {symbol if 'symbol' in locals() else 'unknown'}: {e}")
            return None
    
    def fetch_prices_parallel(self, assets):
        if not assets: return []
        max_workers = min(5, len(assets))
        updated_prices = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_asset = {executor.submit(self.fetch_price, asset): asset for asset in assets}
                for future in concurrent.futures.as_completed(future_to_asset, timeout=180):
                    try:
                        result = future.result(timeout=15)
                        if result and isinstance(result, dict): updated_prices.append(result)
                    except Exception: continue
            return updated_prices
        except Exception as e:
            logger.error(f"❌ Error in parallel fetch: {e}")
            return updated_prices
    
    def _fetch_jp_stock(self, symbol):
        """日本株の価格と名称（日本語）を取得"""
        try:
            # 1. 価格取得（API）
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
            
            # 2. 名称取得（日本語サイトからスクレイピング）
            name = f"Stock {symbol}"
            try:
                url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
                response = self.session.get(url, timeout=5)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 複数のセレクタで名前を探す
                    possible_elements = [
                        soup.find('h1', class_='_6uDf3mAC'),  # Yahooファイナンスの一般的なクラス
                        soup.find('h1'),
                        soup.find('title')
                    ]
                    
                    for el in possible_elements:
                        if el:
                            raw = el.get_text(strip=True)
                            # " - Yahoo!ファイナンス" などを除去
                            cleaned = raw.split('【')[0].split(' - ')[0].strip()
                            # (株)などを除去
                            cleaned = cleaned.replace('(株)', '').replace('株)', '').replace('(株', '').strip()
                            if cleaned and not cleaned.isdigit() and len(cleaned) > 0:
                                name = cleaned
                                break
            except Exception as scrape_error:
                logger.warning(f"⚠️ JP Stock name scraping failed for {symbol}: {scrape_error}")

            if price > 0:
                logger.info(f"✅ JP Stock: {name} ({symbol}) = ¥{price:,.0f}")
                return price, name
            
            raise ValueError(f"Price not found for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Error getting JP stock {symbol}: {e}")
            raise

    def _fetch_us_stock(self, symbol):
        # ... (変更なし)
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            response = self.session.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                price = (result['meta'].get('regularMarketPrice') or result['meta'].get('previousClose') or 0)
                name = result['meta'].get('shortName') or symbol.upper()
                if price > 0: return round(float(price), 2), name
            raise ValueError("Price not found")
        except Exception as e:
            logger.error(f"Error US stock: {e}")
            raise

    def _fetch_gold_price(self):
        # ... (変更なし)
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
        # ... (変更なし)
        try:
            symbol = (symbol or '').upper()
            url = f"https://cc.minkabu.jp/pair/{symbol}_JPY"
            text = self.session.get(url, timeout=10).text
            m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*円', text)
            if m: return float(m.group(1).replace(',', '')), symbol
            raise ValueError("Crypto price not found")
        except Exception as e:
            logger.error(f"Error crypto: {e}")
            raise

    def _fetch_investment_trust(self, symbol):
        # ... (変更なし)
        try:
            symbol_map = {'S&P500': 'JP90C000GKC6', 'オルカン': 'JP90C000H1T1', 'FANG+': 'JP90C000FZD4'}
            if symbol not in symbol_map: raise ValueError("Unknown fund")
            url = f"https://www.rakuten-sec.co.jp/web/fund/detail/?ID={symbol_map[symbol]}"
            soup = BeautifulSoup(self.session.get(url, timeout=10).text, 'html.parser')
            th = soup.find('th', string=re.compile(r'基準価額'))
            if th and th.find_next_sibling('td'):
                val = re.search(r'([0-9,]+)', th.find_next_sibling('td').get_text())
                if val: return float(val.group(1).replace(',', '')), symbol
            raise ValueError("Fund price not found")
        except Exception as e:
            logger.error(f"Error fund: {e}")
            raise

    def get_usd_jpy_rate(self):
        try:
            cached = self.cache.get("USD_JPY")
            if cached: return cached['rate']
            api_url = "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY=X"
            data = self.session.get(api_url, timeout=10).json()
            rate = data['chart']['result'][0]['meta']['regularMarketPrice']
            self.cache.set("USD_JPY", {'rate': rate})
            return rate
        except: return 150.0

from config import get_config
price_service = PriceService(get_config())
