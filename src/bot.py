from functools import partial
from itertools import islice
from binance import Client, ThreadedWebsocketManager
from binance.streams import ReconnectingWebsocket

ReconnectingWebsocket.MAX_RECONNECTS = 0  # prevent reconnect
ReconnectingWebsocket.MAX_QUEUE_SIZE = 100000

MARKET_TYPE_SPOT = 'spot'
MARKET_TYPE_PERP = 'perp'


class Bot:
    def __init__(self, spot_symbols, perp_symbols, logger,
                 spot_uploader, perp_uploader):
        self.perp_symbols = perp_symbols[:]
        self.spot_symbols = spot_symbols[:]
        self.finished = False
        self.logger = logger
        self.spot_uploader = spot_uploader
        self.perp_uploader = perp_uploader
        self.client = Client()

        # fetch old data
        for symbol in spot_symbols:
            self._fetch_historical(MARKET_TYPE_SPOT, symbol)
        for symbol in perp_symbols:
            self._fetch_historical(MARKET_TYPE_PERP, symbol)

        self.twm = ThreadedWebsocketManager()
        self.twm.start()

        streams = [f'{s.lower()}@kline_1m' for s in spot_symbols]
        self.twm.start_multiplex_socket(
            callback=partial(self._handle_socket_message, MARKET_TYPE_SPOT),
            streams=streams
        )
        streams = [f'{s.lower()}@kline_1m' for s in perp_symbols]
        self.twm.start_futures_multiplex_socket(
            callback=partial(self._handle_socket_message, MARKET_TYPE_PERP),
            streams=streams
        )

        self.spot_historical_fetched = set()
        self.perp_historical_fetched = set()

    def join(self):
        self.logger.info('join')
        self.twm.stop()
        self.twm.join()

    # called from other thread
    # partial kline comes
    def _handle_socket_message(self, market_type, msg):
        try:
            self.logger.debug(f'{market_type} {msg}')

            data = msg['data']
            if data['e'] == 'error':
                self.logger.error(msg)
                self.logger.error('websocket error. finish bot')
                self.finished = True
                return

            if self.finished:
                self.logger.debug('message skipped because finished')
                return

            if data['e'] != 'kline':
                return

            if market_type == MARKET_TYPE_SPOT:
                historical_fetched = self.spot_historical_fetched
            else:
                historical_fetched = self.perp_historical_fetched

            symbol = data['s']
            if symbol not in historical_fetched:
                # fetch data arrived between [fetch old data, first message]
                self._fetch_historical(market_type, symbol)
                historical_fetched.add(symbol)

            self._get_uploader(market_type).add(symbol, [_ws_kline_to_rest(data['k'])])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.logger.error('websocket handler error. finish bot')
            self.finished = True

    def _fetch_historical(self, market_type, symbol):
        uploader = self._get_uploader(market_type)
        last_timestamp = uploader.get_last_timestamp(symbol)
        self.logger.info(f'fetch historical {market_type} {symbol} last_timestamp {last_timestamp}')
        start_time = (last_timestamp + 1) * 1000
        if market_type == MARKET_TYPE_SPOT:
            klines = self.client.get_historical_klines_generator(
                symbol,
                Client.KLINE_INTERVAL_1MINUTE,
                start_time,
            )
        else:
            klines = self.client.futures_historical_klines_generator(
                symbol,
                Client.KLINE_INTERVAL_1MINUTE,
                start_time,
            )

        while True:
            chunk = list(islice(klines, 28 * 24 * 60))
            if not chunk:
                break
            uploader.add(symbol, chunk)

    def _get_uploader(self, market_type):
        if market_type == MARKET_TYPE_SPOT:
            return self.spot_uploader
        else:
            return self.perp_uploader


def _ws_kline_to_rest(x):
    return [
        x['t'], # Kline start time
        x['o'], # ohlcv
        x['h'],
        x['l'],
        x['c'],
        x['v'],
        x['T'], # Kline close time
        x['q'], # Quote asset volume
        x['n'], # Number of trades
        x['V'], # Taker buy base asset volume
        x['Q'], # Taker buy quote asset volume
        x['B'], # Unused field, ignore.
    ]
