import requests
from bs4 import BeautifulSoup
import time
import concurrent.futures
from utils import logger, cache

class PriceService:
    def __init__(self, config):
        self.config = config
        self.cache = cache.SimpleCache(duration=300)  # 5分キャッシュ
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
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
                # ✅ エラー時は現在の価格を維持してスキップ
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
            logger.error(f"❌ Error fetching price for {symbol if 'symbol' in locals() else 'unknown'}: {e}")
            return None
    
    def fetch_prices_parallel(self, assets):
        """複数資産の価格を並列取得"""
        if not assets:
            logger.warning("⚠️ No assets to fetch prices for")
            return []
        
        # ✅ ワーカー数を制限（タイムアウト対策）
        max_workers = min(10, len(assets))  # 20 → 10 に削減
        updated_prices = []
        
        logger.info(f"🔄 Starting parallel price fetch for {len(assets)} assets with {max_workers} workers")
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # ✅ タイムアウトを設定
                future_to_asset = {executor.submit(self.fetch_price, asset): asset for asset in assets}
                
                for future in concurrent.futures.as_completed(future_to_asset, timeout=240):
                    try:
                        result = future.result(timeout=10)  # 個別タイムアウト10秒
                        if result is not None and isinstance(result, dict):
                            updated_prices.append(result)
                    except concurrent.futures.TimeoutError:
                        asset = future_to_asset[future]
                        logger.warning(f"⚠️ Timeout fetching price for {asset.get('symbol', 'unknown')}")
                    except Exception as e:
                        asset = future_to_asset[future]
                        logger.warning(f"⚠️ Error in future for {asset.get('symbol', 'unknown')}: {e}")
            
            logger.info(f"✅ Completed parallel fetch: {len(updated_prices)}/{len(assets)} prices updated")
            return updated_prices
        
        except concurrent.futures.TimeoutError:
            logger.error(f"❌ Overall timeout in parallel fetch")
            return updated_prices  # 取得できた分だけ返す
        
        except Exception as e:
            logger.error(f"❌ Error in parallel fetch: {e}", exc_info=True)
            return updated_prices
    
    def _fetch_jp_stock(self, symbol):
        """日本株の価格を取得（Yahoo Finance Japan）"""
        try:
            url = f"https://finance.yahoo.co.jp/quote/{symbol}.T"
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # ✅ 複数のセレクタを試す
            price_elem = None
            selectors = [
                'span._3BGK5SVf',
                'span.stoksPrice',
                'span[class*="price"]',
                'div[class*="price"]'
            ]
            
            for selector in selectors:
                price_elem = soup.select_one(selector)
                if price_elem:
                    break
            
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '')
                price = float(price_text)
            else:
                raise ValueError(f"Price element not found for {symbol}")
            
            # 銘柄名取得
            name_elem = soup.select_one('h1._1jTcLIqL')
            if not name_elem:
                name_elem = soup.select_one('h1')
            
            name = name_elem.text.strip() if name_elem else symbol
            
            return price, name
        
        except Exception as e:
            logger.error(f"❌ Error getting JP stock {symbol}: {e}")
            raise
    
    def _fetch_us_stock(self, symbol):
        """米国株の価格を取得（Yahoo Finance US）"""
        try:
            url = f"https://finance.yahoo.com/quote/{symbol}"
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 価格取得
            price_elem = soup.select_one('fin-streamer[data-symbol="{}"][data-field="regularMarketPrice"]'.format(symbol))
            if not price_elem:
                price_elem = soup.select_one('fin-streamer[data-field="regularMarketPrice"]')
            
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
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 金価格取得（買取価格）
            price_elem = soup.select_one('table.table_main tr:nth-of-type(2) td:nth-of-type(3)')
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '')
                price = float(price_text)
            else:
                raise ValueError("Gold price element not found")
            
            return price, "金(Gold)"
        
        except Exception as e:
            logger.error(f"❌ Error getting gold price: {e}")
            raise
    
    def _fetch_crypto(self, symbol):
        """暗号資産の価格を取得（みんかぶ暗号資産）"""
        try:
            symbol_map = {
                'BTC': 'btc',
                'ETH': 'eth',
                'XRP': 'xrp',
                'DOGE': 'doge'
            }
            
            symbol_lower = symbol_map.get(symbol.upper(), symbol.lower())
            url = f"https://cc.minkabu.jp/pair/{symbol_lower}_jpy"
            
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 価格取得
            price_elem = soup.select_one('div.md_price')
            if not price_elem:
                price_elem = soup.select_one('span.price')
            
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('¥', '').replace('円', '')
                price = float(price_text)
            else:
                raise ValueError(f"Crypto price element not found for {symbol}")
            
            # 名前取得
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
            
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 基準価額取得
            price_elem = soup.select_one('span.value')
            if not price_elem:
                price_elem = soup.select_one('dd.fund-detail-nav')
            
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '').replace('円', '')
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
            response = self.session.get(url, timeout=self.config.API_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            price_elem = soup.select_one('span._3BGK5SVf')
            if not price_elem:
                price_elem = soup.select_one('span.stoksPrice')
            
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
