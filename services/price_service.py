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
                return None
            
            asset_id = asset_dict['id']
            asset_type = asset_dict['asset_type']
            symbol = asset_dict['symbol']
            
            if asset_type in ['cash', 'insurance']:
                return None
            
            # キャッシュチェック
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
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch price for {symbol}: {e}")
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
        try:
            # 1. 名称取得: Yahoo!ファイナンス(日本)をスクレイピング
            scrape_url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
            response = self.session.get(scrape_url, timeout=10)
            
            name = f"Stock {symbol}"
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 名称取得の優先順位: H1 > Title
                target_element = soup.find('h1')
                if not target_element:
                    target_element = soup.find('title')
                
                if target_element:
                    raw_name = target_element.get_text(strip=True)
                    
                    # 不要な文字を削除するクリーニング処理
                    # 1. 区切り文字以降を削除 ("【", " - ")
                    cleaned = raw_name.split('【')[0].split(' - ')[0]
                    
                    # 2. 特定のフレーズを削除 (正規表現で強力に除去)
                    # "の株価", "・株式情報", "株価" など
                    cleaned = re.sub(r'(の?株価[・･]?株式情報|の?株価|株式情報).*$', '', cleaned)
                    
                    # 3. 会社種別を削除
                    cleaned = cleaned.replace('(株)', '').replace('株)', '').replace('(株', '').strip()
                    
                    if cleaned:
                        name = cleaned
                        logger.info(f"✅ Cleaned JP name: {name}")
            
            # 2. 価格取得: APIを使用
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
            
            if price > 0:
                return price, name
            
            raise ValueError(f"Price not found for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Error getting JP stock {symbol}: {e}")
            raise

    def _fetch_us_stock(self, symbol):
        """米国株の価格を取得"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            response = self.session.get(api_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                
                price_usd = 0
                if 'meta' in result:
                    meta = result['meta']
                    price_usd = (meta.get('regularMarketPrice') or 
                               meta.get('previousClose') or 
                               meta.get('chartPreviousClose') or 0)
                
                name = symbol.upper()
                if 'meta' in result:
                    name = meta.get('shortName') or meta.get('longName') or symbol.upper()
                
                if price_usd > 0:
                    return round(float(price_usd), 2), name
            
            raise ValueError(f"Price not found for {symbol}")
        
        except Exception as e:
            logger.error(f"❌ Error getting US stock {symbol}: {e}")
            raise
    
    def _fetch_gold_price(self):
        """金価格を取得"""
        try:
            url = "https://gold.tanaka.co.jp/commodity/souba/english/index.php"
            response = self.session.get(url, timeout=10)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) > 1 and tds[0].get_text(strip=True).upper() == 'GOLD':
                    price_text = tds[1].get_text(strip=True)
                    price_match = re.search(r'([0-9,]+)\s*yen', price_text)
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                        return price, "金(Gold)"
            raise ValueError("Gold price not found")
        except Exception as e:
            logger.error(f"❌ Error getting gold price: {e}")
            raise
    
    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得（API不使用・スクレイピング強化版）"""
        try:
            symbol = (symbol or '').upper()
            # みんかぶのURL (例: https://cc.minkabu.jp/pair/BTC_JPY)
            url = f"https://cc.minkabu.jp/pair/{symbol}_JPY"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            text = response.text
            soup = BeautifulSoup(text, 'html.parser')
            
            price = 0.0
            
            # パターン1: data-price 属性を探す
            # <div ... data-price="9000000" ...>
            price_element = soup.find(attrs={"data-price": True})
            if price_element:
                try:
                    price = float(price_element['data-price'])
                    logger.info(f"✅ Crypto {symbol} found via data-price: {price}")
                except:
                    pass

            # パターン2: 特定のクラスのテキストを探す (.pair_price, .price, .stock_price)
            if price <= 0:
                selectors = [
                    'div.pair_price', 
                    'span.pair_price', 
                    'div.price', 
                    'span.price', 
                    'div.stock_price'
                ]
                for selector in selectors:
                    element = soup.select_one(selector)
                    if element:
                        # "9,000,000 円" のようなテキストから数値のみ抽出
                        text_val = element.get_text(strip=True)
                        m = re.search(r'([0-9,]+(?:\.[0-9]+)?)', text_val)
                        if m:
                            try:
                                price = float(m.group(1).replace(',', ''))
                                logger.info(f"✅ Crypto {symbol} found via selector {selector}: {price}")
                                break
                            except:
                                pass
            
            # パターン3: JSON-LDやスクリプト内のデータを探す (最終手段)
            if price <= 0:
                matches = re.findall(r'"price"\s*:\s*"?([0-9\.,]+)"?', text)
                for m in matches:
                    try:
                        p = float(m.replace(',', ''))
                        if p > 0:
                            price = p
                            logger.info(f"✅ Crypto {symbol} found via JSON regex: {price}")
                            break
                    except:
                        pass

            if price > 0:
                return round(price, 2), symbol
                
            raise ValueError(f"Crypto price not found for {symbol} on page")
            
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
            
            def extract_num(s):
                if not s: return None
                s = s.replace(',', '').replace(' ', '').replace('円', '')
                m = re.search(r'([+-]?\d+(?:\.\d+)?)', s)
                return float(m.group(1)) if m else None

            th = soup.find('th', string=re.compile(r'\s*基準価額\s*'))
            if th:
                td = th.find_next_sibling('td')
                if td:
                    val = extract_num(td.get_text())
                    if val: return round(val, 2), symbol
            
            for selector in ['.price', '.nav', 'dd.fund-detail-nav', 'td.alR']:
                for elem in soup.select(selector):
                    val = extract_num(elem.get_text())
                    if val and 1000 <= val <= 100000: return round(val, 2), symbol
            
            raise ValueError(f"Fund price not found for {symbol}")
        except Exception as e:
            logger.error(f"❌ Error getting investment trust {symbol}: {e}")
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
