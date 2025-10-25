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
            return updated_prices  # 取得できた分だけ返す
        
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
        """暗号資産の価格を取得（複数APIフォールバック）"""
        try:
            # ✅ 方法1: Binance Public API（無料・制限緩い）
            try:
                symbol_map = {
                    'BTC': 'BTCUSDT',
                    'ETH': 'ETHUSDT',
                    'XRP': 'XRPUSDT',
                    'DOGE': 'DOGEUSDT'
                }
                
                pair = symbol_map.get(symbol.upper())
                if not pair:
                    raise ValueError(f"Unsupported crypto: {symbol}")
                
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={pair}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                usd_price = float(data['price'])
                
                # USD→JPY変換
                jpy_rate = self.get_usd_jpy_rate()
                price = usd_price * jpy_rate
                
                name_map = {
                    'BTC': 'ビットコイン',
                    'ETH': 'イーサリアム',
                    'XRP': 'リップル',
                    'DOGE': 'ドージコイン'
                }
                name = name_map.get(symbol.upper(), symbol)
                
                logger.info(f"✅ Crypto from Binance: {symbol} = ${usd_price:.2f} (¥{price:,.0f})")
                return round(price, 2), name
            
            except Exception as binance_error:
                logger.warning(f"⚠️ Binance API failed for {symbol}: {binance_error}")
                
                # ✅ 方法2: CoinCap API（フォールバック）
                symbol_map = {
                    'BTC': 'bitcoin',
                    'ETH': 'ethereum',
                    'XRP': 'ripple',
                    'DOGE': 'dogecoin'
                }
                
                coin_id = symbol_map.get(symbol.upper())
                url = f"https://api.coincap.io/v2/assets/{coin_id}"
                
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()['data']
                usd_price = float(data['priceUsd'])
                
                jpy_rate = self.get_usd_jpy_rate()
                price = usd_price * jpy_rate
                
                name_map = {
                    'BTC': 'ビットコイン',
                    'ETH': 'イーサリアム',
                    'XRP': 'リップル',
                    'DOGE': 'ドージコイン'
                }
                name = name_map.get(symbol.upper(), symbol)
                
                logger.info(f"✅ Crypto from CoinCap: {symbol} = ${usd_price:.2f} (¥{price:,.0f})")
                return round(price, 2), name
        
        except Exception as e:
            logger.error(f"❌ Error getting crypto {symbol}: {e}")
            raise
    
    def _fetch_investment_trust(self, symbol):
        """投資信託の価格を取得（楽天証券）"""
        try:
            symbol_map = {
                'S&P500': 'JP90C000GKC6',
                'オルカン': 'JP90C000H1T1',
                'FANG+': 'JP90C000FZD4'
            }
            
            code = symbol_map.get(symbol, symbol)
            url = f"https://www.rakuten-sec.co.jp/web/fund/detail/?ID={code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # "基準価額"を含むthタグを探す
            th = soup.find('th', string=re.compile(r'\s*基準価額\s*'))
            
            if th:
                td = th.find_next_sibling('td')
                if td:
                    price_text = td.get_text(strip=True)
                    # 正規表現で数値のみを抽出
                    numeric_text = re.sub(r'[^0-9.]', '', price_text)
                    if numeric_text:
                        try:
                            price = float(numeric_text)
                            if 1000 <= price <= 100000:  # 妥当な範囲
                                logger.info(f"✅ Investment trust: {symbol} = ¥{price:,.2f}")
                                return round(price, 2), symbol
                        except ValueError:
                            pass
            
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
