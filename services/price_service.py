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
        
        # ✅ User-Agentをランダム化
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
            # ✅ 修正: assetを辞書型に変換
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
            
            # 現金と保険は価格取得不要
            if asset_type in ['cash', 'insurance']:
                return None
            
            # キャッシュチェック
            cache_key = f"{asset_type}:{symbol}"
            cached = self.cache.get(cache_key)
            if cached:
                logger.debug(f"💾 Using cached price for {symbol}")
                return {
                    'id': asset_id,
                    'symbol': symbol,
                    'price': cached['price'],
                    'name': cached.get('name', symbol)
                }
            
            # ✅ リクエスト間にランダムな遅延を追加（Bot対策）
            time.sleep(random.uniform(0.5, 1.5))
            self._update_user_agent()
            
            # 価格取得
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
                # ✅ エラー時は価格取得をスキップ（データベースの既存価格を維持）
                logger.warning(f"⚠️ Failed to fetch price for {symbol}, skipping: {fetch_error}")
                return None
            
            # キャッシュに保存
            self.cache.set(cache_key, {'price': price, 'name': name})
            
            # ✅ 必ず辞書型で返す
            result = {
                'id': asset_id,
                'symbol': symbol,
                'price': price,
                'name': name
            }
            
            logger.info(f"✅ Fetched price for {symbol}: ¥{price:,.2f}")
            return result
        
        except Exception as e:
            logger.warning(f"⚠️ Error fetching price for {symbol if 'symbol' in locals() else 'unknown'}: {e}")
            return None
    
    def fetch_prices_parallel(self, assets):
        """複数資産の価格を並列取得"""
        if not assets:
            logger.warning("⚠️ No assets to fetch prices for")
            return []
        
        # ✅ ワーカー数を削減（Bot対策 + タイムアウト対策）
        max_workers = min(5, len(assets))
        updated_prices = []
        
        logger.info(f"🔄 Starting parallel price fetch for {len(assets)} assets with {max_workers} workers")
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # ✅ 個別タイムアウトを設定
                future_to_asset = {executor.submit(self.fetch_price, asset): asset for asset in assets}
                
                completed = 0
                for future in concurrent.futures.as_completed(future_to_asset, timeout=180):  # 3分
                    completed += 1
                    try:
                        result = future.result(timeout=15)  # 個別タイムアウト15秒
                        if result is not None and isinstance(result, dict):
                            updated_prices.append(result)
                            logger.info(f"✅ Progress: {completed}/{len(assets)}")
                    except concurrent.futures.TimeoutError:
                        asset = future_to_asset[future]
                        logger.warning(f"⚠️ Timeout fetching price for {asset.get('symbol', 'unknown')}")
                    except Exception as e:
                        asset = future_to_asset[future]
                        logger.warning(f"⚠️ Error in future for {asset.get('symbol', 'unknown')}: {e}")
            
            logger.info(f"✅ Completed parallel fetch: {len(updated_prices)}/{len(assets)} prices updated")
            return updated_prices
        
        except concurrent.futures.TimeoutError:
            logger.warning(f"⚠️ Overall timeout in parallel fetch, returning {len(updated_prices)} results")
            return updated_prices
        
        except Exception as e:
            logger.error(f"❌ Error in parallel fetch: {e}", exc_info=True)
            return updated_prices
    
    def _fetch_jp_stock(self, symbol):
        """日本株の価格を取得（Yahoo Finance API）"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.T"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = self.session.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                
                # 価格取得
                price = 0
                if 'meta' in result:
                    meta = result['meta']
                    price = (meta.get('regularMarketPrice') or 
                           meta.get('previousClose') or 
                           meta.get('chartPreviousClose') or 0)
                
                # 銘柄名取得
                name = f"Stock {symbol}"
                if 'meta' in result:
                    meta = result['meta']
                    name = meta.get('shortName') or meta.get('longName') or f"Stock {symbol}"
                    
                    # 会社名のクリーンアップ
                    jp_suffixes = ['株式会社', '合同会社', '合名会社', '合資会社', '有限会社', '(株)', '(株)']
                    for suffix in jp_suffixes:
                        name = name.replace(suffix, '')
                    
                    en_suffixes = [' COMPANY, LIMITED', ' COMPANY LIMITED', ' CO., LTD.', ' CO.,LTD.', 
                                 ' CO., LTD', ' CO.,LTD', ' Co., Ltd.', ' CO.LTD', ' LTD.', ' LTD', 
                                 ' INC.', ' INC', ' CORP.', ' CORP']
                    for suffix in en_suffixes:
                        if name.upper().endswith(suffix):
                            name = name[:-len(suffix)]
                            break
                    name = name.strip()
                
                if price > 0:
                    return round(float(price), 2), name
            
            raise ValueError(f"Price not found for {symbol}")
        
        except Exception as e:
            logger.error(f"❌ Error getting JP stock {symbol}: {e}")
            raise
    
    def _fetch_us_stock(self, symbol):
        """米国株の価格を取得（Yahoo Finance API - USDで返す）"""
        try:
            api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = self.session.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                
                # 価格取得（USD）
                price_usd = 0
                if 'meta' in result:
                    meta = result['meta']
                    price_usd = (meta.get('regularMarketPrice') or 
                               meta.get('previousClose') or 
                               meta.get('chartPreviousClose') or 0)
                
                # 銘柄名取得
                name = symbol.upper()
                if 'meta' in result:
                    meta = result['meta']
                    name = meta.get('shortName') or meta.get('longName') or symbol.upper()
                
                if price_usd > 0:
                    logger.info(f"✅ US Stock: {symbol} = ${price_usd:.2f}")
                    # ✅ USDのまま返す（旧コードと同じ）
                    return round(float(price_usd), 2), name
            
            raise ValueError(f"Price not found for {symbol}")
        
        except Exception as e:
            logger.error(f"❌ Error getting US stock {symbol}: {e}")
            raise
    
    def _fetch_gold_price(self):
        """金価格を取得（田中貴金属）"""
        try:
            url = "https://gold.tanaka.co.jp/commodity/souba/english/index.php"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # GOLD行を探す
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) > 1 and tds[0].get_text(strip=True).upper() == 'GOLD':
                    price_text = tds[1].get_text(strip=True)
                    price_match = re.search(r'([0-9,]+)\s*yen', price_text)
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                        logger.info(f"✅ Gold price: ¥{price:,}")
                        return price, "金(Gold)"
            
            raise ValueError("Gold price element not found")
        
        except Exception as e:
            logger.error(f"❌ Error getting gold price: {e}")
            raise
    
    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得（みんかぶ暗号資産 - 旧コードと同じ）"""
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
    
    def _fetch_investment_trust(self, symbol):
        """投資信託の価格を取得（楽天証券 - 旧コードと同じ）"""
        try:
            # 銘柄コードマッピング（旧コードと同じ）
            symbol_map = {
                'S&P500': 'JP90C000GKC6',
                'オルカン': 'JP90C000H1T1',
                'FANG+': 'JP90C000FZD4'
            }
            
            if symbol not in symbol_map:
                logger.warning(f"Unsupported investment trust symbol: {symbol}")
                raise ValueError(f"Unsupported investment trust: {symbol}")
            
            fund_id = symbol_map[symbol]
            url = f"https://www.rakuten-sec.co.jp/web/fund/detail/?ID={fund_id}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            response.encoding = response.apparent_encoding
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # ヘルパー関数: 文字列から数値を抽出
            def extract_number_from_string(s):
                if not s:
                    return None
                # カンマ、空白、全角スペースを削除
                s = s.replace(',', '').replace(' ', '').replace('\xa0', '').replace('円', '').replace('　', '')
                # 数値パターンを検索
                m = re.search(r'([+-]?\d+(?:\.\d+)?)', s)
                if m:
                    try:
                        return float(m.group(1))
                    except Exception:
                        return None
                return None
            
            # ✅ 方法1: "基準価額"を含むthタグを探す
            th = soup.find('th', string=re.compile(r'\s*基準価額\s*'))
            if th:
                td = th.find_next_sibling('td')
                if td:
                    price_text = td.get_text(strip=True)
                    price = extract_number_from_string(price_text)
                    if price is not None and 1000 <= price <= 100000:  # 妥当な範囲
                        logger.info(f"✅ Investment trust (基準価額): {symbol} = ¥{price:,.2f}")
                        return round(price, 2), symbol
            
            # ✅ 方法2: class="value"やclass="nav"を探す
            selectors = [
                'span.value',
                'dd.fund-detail-nav',
                'span[class*="nav"]',
                'div[class*="price"] span',
                'td.alR',  # 楽天証券の基準価額
                '.price',
                '.nav'
            ]
            
            for selector in selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        price = extract_number_from_string(text)
                        if price is not None and 1000 <= price <= 100000:
                            logger.info(f"✅ Investment trust (selector {selector}): {symbol} = ¥{price:,.2f}")
                            return round(price, 2), symbol
                except Exception:
                    continue
            
            # ✅ 方法3: テーブルの中から「基準価額」を探す
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if '基準価額' in cell_text and i + 1 < len(cells):
                            next_cell = cells[i + 1]
                            price_text = next_cell.get_text(strip=True)
                            price = extract_number_from_string(price_text)
                            if price is not None and 1000 <= price <= 100000:
                                logger.info(f"✅ Investment trust (table): {symbol} = ¥{price:,.2f}")
                                return round(price, 2), symbol
            
            # ✅ 方法4: ページ全体から数値を探す（最後の手段）
            all_text = soup.get_text()
            # "基準価額"の後ろから数値を探す
            idx = all_text.find('基準価額')
            if idx != -1:
                snippet = all_text[idx:idx + 500]
                # "円"の前の数値を探す
                matches = re.findall(r'([0-9,]+(?:\.[0-9]+)?)\s*円', snippet)
                for match in matches:
                    price = extract_number_from_string(match)
                    if price is not None and 1000 <= price <= 100000:
                        logger.info(f"✅ Investment trust (text search): {symbol} = ¥{price:,.2f}")
                        return round(price, 2), symbol
            
            # すべて失敗
            logger.warning(f"⚠️ Could not find the price for {symbol} on the page. HTML structure may have changed.")
            raise ValueError(f"Investment trust price not found for {symbol}")
        
        except Exception as e:
            logger.error(f"❌ Error getting investment trust {symbol}: {e}")
            raise
    
    def get_usd_jpy_rate(self):
        """USD/JPY為替レートを取得"""
        try:
            cache_key = "USD_JPY"
            cached = self.cache.get(cache_key)
            if cached:
                return cached['rate']
            
            api_url = "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY=X"
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            response = self.session.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                if 'meta' in result and 'regularMarketPrice' in result['meta']:
                    rate = float(result['meta']['regularMarketPrice'])
                    self.cache.set(cache_key, {'rate': rate})
                    logger.info(f"✅ USD/JPY rate: {rate:.2f}")
                    return rate
            
            logger.warning("⚠️ Could not fetch USD/JPY rate, using default: 150.0")
            return 150.0
        
        except Exception as e:
            logger.warning(f"⚠️ Error getting USD/JPY rate: {e}")
            return 150.0

# グローバルインスタンス
from config import get_config
price_service = PriceService(get_config())
