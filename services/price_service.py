import requests
from bs4 import BeautifulSoup
import time
import random
import concurrent.futures
from utils import logger, cache

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
        
        # ✅ ワーカー数をさらに削減（Bot対策 + タイムアウト対策）
        max_workers = min(5, len(assets))  # 10 → 5 に削減
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
        """日本株の価格を取得（Yahoo Finance Japan）"""
        try:
            url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # ✅ より多くのセレクタを試す
            price_elem = None
            selectors = [
                'span._3BGK5SVf',
                'span.stoksPrice',
                'span[class*="price"]',
                'div[class*="price"] span',
                'dd[class*="price"]',
                'span[data-test="qsp-price"]'
            ]
            
            for selector in selectors:
                price_elem = soup.select_one(selector)
                if price_elem and price_elem.text.strip():
                    break
            
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '').replace(' ', '')
                price = float(price_text)
            else:
                raise ValueError(f"Price element not found for {symbol}")
            
            # 銘柄名取得
            name_elem = soup.select_one('h1._1jTcLIqL') or soup.select_one('h1')
            name = name_elem.text.strip() if name_elem else symbol
            
            return price, name
        
        except Exception as e:
            logger.error(f"❌ Error getting JP stock {symbol}: {e}")
            raise
    
    def _fetch_us_stock(self, symbol):
        """米国株の価格を取得（Yahoo Finance US）"""
        try:
            url = f"https://finance.yahoo.com/quote/{symbol}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # ✅ 複数のセレクタを試す
            price_elem = None
            selectors = [
                f'fin-streamer[data-symbol="{symbol}"][data-field="regularMarketPrice"]',
                'fin-streamer[data-field="regularMarketPrice"]',
                'span[data-test="qsp-price"]',
                'div[data-test="qsp-price"] span'
            ]
            
            for selector in selectors:
                price_elem = soup.select_one(selector)
                if price_elem and price_elem.text.strip():
                    break
            
            if price_elem:
                price = float(price_elem.text.strip().replace(',', ''))
            else:
                raise ValueError(f"Price element not found for {symbol}")
            
            # 銘柄名取得
            name_elem = soup.select_one('h1')
            name = name_elem.text.strip().split('(')[0].strip() if name_elem else symbol
            
            return price, name
        
        except Exception as e:
            logger.error(f"❌ Error getting US stock {symbol}: {e}")
            raise
    
    def _fetch_gold_price(self):
        """金価格を取得（田中貴金属）"""
        try:
            url = "https://gold.tanaka.co.jp/commodity/souba/m-gold.php"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 金価格取得（買取価格）
            price_elem = soup.select_one('table.table_main tr:nth-of-type(2) td:nth-of-type(3)')
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '').replace(' ', '')
                price = float(price_text)
            else:
                raise ValueError("Gold price element not found")
            
            return price, "金(Gold)"
        
        except Exception as e:
            logger.error(f"❌ Error getting gold price: {e}")
            raise
    
    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得（CoinGecko APIに変更）"""
        try:
            # ✅ みんかぶの代わりにCoinGecko APIを使用（より安定）
            symbol_map = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'XRP': 'ripple',
                'DOGE': 'dogecoin'
            }
            
            coin_id = symbol_map.get(symbol.upper(), symbol.lower())
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=jpy"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if coin_id in data and 'jpy' in data[coin_id]:
                price = float(data[coin_id]['jpy'])
            else:
                raise ValueError(f"Crypto price not found for {symbol}")
            
            # 名前マッピング
            name_map = {
                'BTC': 'ビットコイン',
                'ETH': 'イーサリアム',
                'XRP': 'リップル',
                'DOGE': 'ドージコイン'
            }
            name = name_map.get(symbol.upper(), symbol)
            
            return price, name
        
        except Exception as e:
            logger.error(f"❌ Error getting crypto {symbol}: {e}")
            raise
    
    def _fetch_investment_trust(self, symbol):
        """投資信託の価格を取得（楽天証券）"""
        try:
            symbol_map = {
                'S&P500': '2558',
                'オルカン': '03311187',
                'FANG+': '03312187'
            }
            
            code = symbol_map.get(symbol, symbol)
            url = f"https://www.rakuten-sec.co.jp/web/fund/detail/?ID={code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # ✅ 複数のセレクタを試す
            price_elem = None
            selectors = [
                'span.value',
                'dd.fund-detail-nav',
                'span[class*="nav"]',
                'div[class*="price"] span'
            ]
            
            for selector in selectors:
                price_elem = soup.select_one(selector)
                if price_elem and price_elem.text.strip():
                    break
            
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '').replace(' ', '')
                price = float(price_text)
            else:
                raise ValueError(f"Investment trust price element not found for {symbol}")
            
            return price, symbol
        
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
            
            url = "https://finance.yahoo.co.jp/quote/USDJPY=X"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            price_elem = soup.select_one('span._3BGK5SVf') or soup.select_one('span.stoksPrice')
            
            if price_elem:
                rate = float(price_elem.text.strip().replace(',', ''))
                self.cache.set(cache_key, {'rate': rate})
                logger.info(f"✅ USD/JPY rate: {rate}")
                return rate
            
            logger.warning("⚠️ Could not fetch USD/JPY rate, using default: 150.0")
            return 150.0
        
        except Exception as e:
            logger.warning(f"⚠️ Error getting USD/JPY rate: {e}")
            return 150.0

# グローバルインスタンス
from config import get_config
price_service = PriceService(get_config())
