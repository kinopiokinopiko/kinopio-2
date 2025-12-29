import requests
from bs4 import BeautifulSoup
import time
import random
import concurrent.futures
from utils import logger, cache
import re
import json

class PriceService:
    def __init__(self, config):
        self.config = config
        self.cache = cache.SimpleCache(duration=300)  # 5分キャッシュ
        self.session = requests.Session()
        
        # User-Agentをランダム化 (PCブラウザとして振る舞う)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self._update_user_agent()
    
    def _update_user_agent(self):
        """User-Agentをランダムに更新"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
    
    def fetch_price(self, asset):
        """単一資産の価格を取得"""
        try:
            if hasattr(asset, 'keys'): asset_dict = dict(asset)
            elif isinstance(asset, dict): asset_dict = asset
            else: return None
            
            asset_type = asset_dict['asset_type']
            symbol = asset_dict['symbol']
            
            if asset_type in ['cash', 'insurance']: return None
            
            # キャッシュチェック
            cache_key = f"{asset_type}:{symbol}"
            cached = self.cache.get(cache_key)
            if cached:
                return {
                    'id': asset_dict['id'],
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
            
            if price > 0:
                self.cache.set(cache_key, {'price': price, 'name': name})
                return {'id': asset_dict['id'], 'symbol': symbol, 'price': price, 'name': name}
            
            return None
        
        except Exception as e:
            logger.error(f"❌ Error in fetch_price: {e}", exc_info=True)
            return None
    
    def fetch_prices_parallel(self, assets):
        """並列取得"""
        if not assets: return []
        max_workers = min(5, len(assets))
        updated_prices = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_asset = {executor.submit(self.fetch_price, asset): asset for asset in assets}
                for future in concurrent.futures.as_completed(future_to_asset, timeout=180):
                    try:
                        result = future.result(timeout=15)
                        if result: updated_prices.append(result)
                    except Exception: continue
            return updated_prices
        except Exception as e:
            logger.error(f"❌ Parallel fetch error: {e}")
            return updated_prices

    def _fetch_jp_stock(self, symbol):
        """日本株 (Yahoo!ファイナンス)"""
        try:
            # 1. 名称取得 (スクレイピング)
            url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
            response = self.session.get(url, timeout=10)
            name = f"Stock {symbol}"
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # タイトルタグからの抽出
                # 例: <title>(株)エス・サイエンス【5721】：株価・株式情報 - Yahoo!ファイナンス</title>
                title_tag = soup.find('title')
                if title_tag:
                    raw_title = title_tag.get_text(strip=True)
                    logger.debug(f"🔍 Raw JP Title: {raw_title}")
                    
                    # '【' で分割して左側を取得 -> "(株)エス・サイエンス"
                    if '【' in raw_title:
                        name_part = raw_title.split('【')[0]
                        # (株)などを除去
                        cleaned_name = name_part.replace('(株)', '').replace('（株）', '').strip()
                        if cleaned_name:
                            name = cleaned_name
                            logger.info(f"✅ Extracted JP Name from Title: {name}")
            
            # 2. 価格取得 (API)
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
            raise ValueError("Price not found")
            
        except Exception as e:
            logger.error(f"❌ JP Stock Error ({symbol}): {e}")
            raise

    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得（みんかぶ暗号資産）"""
        try:
            symbol = (symbol or '').upper()
            
            # サポートされている銘柄チェック
            supported_symbols = ['BTC', 'ETH', 'XRP', 'DOGE']
            if symbol not in supported_symbols:
                logger.warning(f"Unsupported crypto symbol requested: {symbol}")
                raise ValueError(f"Unsupported crypto: {symbol}")
            
            url = f"https://cc.minkabu.jp/pair/{symbol}_JPY"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.encoding = response.apparent_encoding
            text = response.text
            
            # ヘルパー関数: 文字列から数値を抽出
            def extract_number_from_string(s):
                if not s:
                    return None
                # カンマと空白を削除
                s = s.replace(',', '').replace(' ', '').replace('\xa0', '')
                # 数値パターンを検索
                m = re.search(r'([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)', s)
                if m:
                    try:
                        return float(m.group(1))
                    except Exception:
                        return None
                return None
            
            # ✅ 方法1: JSON-likeフィールドから価格を抽出
            json_matches = re.findall(r'"(?:last|price|lastPrice|close|current|ltp)"\s*:\s*"?([0-9\.,Ee+\-]+)"?', text)
            if json_matches:
                for jm in json_matches:
                    val = extract_number_from_string(jm)
                    if val is not None and val > 0:
                        logger.debug(f"Found price in JSON-like field: {jm} -> {val}")
                        name_map = {
                            'BTC': 'ビットコイン',
                            'ETH': 'イーサリアム',
                            'XRP': 'リップル',
                            'DOGE': 'ドージコイン'
                        }
                        name = name_map.get(symbol, symbol)
                        logger.info(f"✅ Crypto from みんかぶ (JSON): {symbol} = ¥{val:,.2f}")
                        return round(val, 2), name
            
            # ✅ 方法2: 「現在値」の近くから価格を抽出
            idx = text.find('現在値')
            if idx != -1:
                snippet = text[idx: idx + 700]
                m = re.search(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*円', snippet)
                if m:
                    val = extract_number_from_string(m.group(1))
                    if val is not None and val > 0:
                        name_map = {
                            'BTC': 'ビットコイン',
                            'ETH': 'イーサリアム',
                            'XRP': 'リップル',
                            'DOGE': 'ドージコイン'
                        }
                        name = name_map.get(symbol, symbol)
                        logger.info(f"✅ Crypto from みんかぶ (現在値): {symbol} = ¥{val:,.2f}")
                        return round(val, 2), name
            
            # ✅ 方法3: data-price属性から抽出
            m = re.search(r'data-price=["\']([0-9\.,Ee+\-]+)["\']', text)
            if m:
                val = extract_number_from_string(m.group(1))
                if val is not None and val > 0:
                    name_map = {
                        'BTC': 'ビットコイン',
                        'ETH': 'イーサリアム',
                        'XRP': 'リップル',
                        'DOGE': 'ドージコイン'
                    }
                    name = name_map.get(symbol, symbol)
                    logger.info(f"✅ Crypto from みんかぶ (data-price): {symbol} = ¥{val:,.2f}")
                    return round(val, 2), name
            
            # ✅ 方法4: BeautifulSoupでCSSセレクタから抽出
            soup = BeautifulSoup(text, 'html.parser')
            selectors = [
                'div.pairPrice', '.pairPrice', '.pair_price', 'div.priceWrap', 
                'div.kv', 'span.yen', 'div.stock_price span.yen', 'p.price', 
                'span.price', 'div.price', 'span.value', 'div.value', 'strong', 'b'
            ]
            
            for sel in selectors:
                try:
                    tag = soup.select_one(sel)
                    if tag:
                        txt = tag.get_text(' ', strip=True)
                        val = extract_number_from_string(txt)
                        if val is not None and val > 0:
                            logger.debug(f"Found price by selector {sel}: {txt} -> {val}")
                            name_map = {
                                'BTC': 'ビットコイン',
                                'ETH': 'イーサリアム',
                                'XRP': 'リップル',
                                'DOGE': 'ドージコイン'
                            }
                            name = name_map.get(symbol, symbol)
                            logger.info(f"✅ Crypto from みんかぶ (selector {sel}): {symbol} = ¥{val:,.2f}")
                            return round(val, 2), name
                except Exception:
                    continue
            
            # ✅ 方法5: 「円」という文字列の前の数値を抽出
            matches = re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)\s*円', text)
            for num in matches:
                val = extract_number_from_string(num)
                if val is not None and val > 0:
                    name_map = {
                        'BTC': 'ビットコイン',
                        'ETH': 'イーサリアム',
                        'XRP': 'リップル',
                        'DOGE': 'ドージコイン'
                    }
                    name = name_map.get(symbol, symbol)
                    logger.info(f"✅ Crypto from みんかぶ (円): {symbol} = ¥{val:,.2f}")
                    return round(val, 2), name
            
            # ✅ 方法6: 科学的記数法（1.23e+6など）
            m2 = re.search(r'([0-9\.,]+[eE][+-]?\d+)', text)
            if m2:
                val = extract_number_from_string(m2.group(1))
                if val is not None and val > 0:
                    logger.debug(f"Found price by scientific notation: {m2.group(1)} -> {val}")
                    name_map = {
                        'BTC': 'ビットコイン',
                        'ETH': 'イーサリアム',
                        'XRP': 'リップル',
                        'DOGE': 'ドージコイン'
                    }
                    name = name_map.get(symbol, symbol)
                    logger.info(f"✅ Crypto from みんかぶ (scientific): {symbol} = ¥{val:,.2f}")
                    return round(val, 2), name
            
            # すべて失敗した場合
            logger.warning(f"⚠️ Failed to parse crypto price for {symbol}")
            snippet = text[:1200].replace('\n', ' ')
            logger.debug(f"HTML snippet:\n{snippet}\n--- end snippet ---")
            
            raise ValueError(f"Crypto price not found for {symbol}")
        
        except Exception as e:
            logger.error(f"❌ Error getting crypto {symbol}: {e}")
            raise

    def _fetch_us_stock(self, symbol):
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            response = self.session.get(api_url, timeout=10)
            data = response.json()
            result = data['chart']['result'][0]
            meta = result['meta']
            price = (meta.get('regularMarketPrice') or meta.get('previousClose') or 0)
            name = meta.get('shortName') or symbol.upper()
            if price > 0: return round(float(price), 2), name
            raise ValueError("Price not found")
        except Exception as e:
            logger.error(f"Error US stock {symbol}: {e}")
            raise

    def _fetch_gold_price(self):
        try:
            url = "https://gold.tanaka.co.jp/commodity/souba/english/index.php"
            soup = BeautifulSoup(self.session.get(url, timeout=10).text, 'html.parser')
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) > 1 and 'GOLD' in tds[0].get_text(strip=True).upper():
                    price = int(re.search(r'([0-9,]+)', tds[1].get_text()).group(1).replace(',', ''))
                    return price, "金(Gold)"
            raise ValueError("Gold price not found")
        except Exception as e:
            logger.error(f"Error gold: {e}")
            raise

    def _fetch_investment_trust(self, symbol):
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
            logger.error(f"Error fund {symbol}: {e}")
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
