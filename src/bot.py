from itertools import islice
from binance import Client, ThreadedWebsocketManager
from binance.streams import ReconnectingWebsocket

ReconnectingWebsocket.MAX_RECONNECTS = 0  # prevent reconnect
ReconnectingWebsocket.MAX_QUEUE_SIZE = 100000

MARKET_TYPE_SPOT = 'spot'
MARKET_TYPE_PERP = 'perp'


class Bot:
    def __init__(self, market_type, symbols, logger, uploader):
        self.symbols = symbols[:]
        self.market_type = market_type
        self.finished = False
        self.logger = logger
        self.uploader = uploader
        self.client = Client()

        # fetch old data
        for symbol in symbols:
            self._fetch_historical(symbol)

        self.twm = ThreadedWebsocketManager()
        self.twm.start()

        streams = [f'{s.lower()}@kline_1m' for s in symbols]
        if market_type == MARKET_TYPE_SPOT:
            self.twm.start_multiplex_socket(
                callback=self._handle_socket_message,
                streams=streams
            )
        else:
            self.twm.start_futures_multiplex_socket(
                callback=self._handle_socket_message,
                streams=streams
            )

        self.historical_fetched = set()

    def join(self):
        self.logger.info('join')
        self.twm.stop()
        self.twm.join()

    # called from other thread
    # partial kline comes
    def _handle_socket_message(self, msg):
        try:
            self.logger.debug(msg)

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

            symbol = data['s']
            if symbol not in self.historical_fetched:
                # fetch data arrived between [fetch old data, first message]
                self._fetch_historical(symbol)
                self.historical_fetched.add(symbol)

            self.uploader.add(symbol, [_ws_kline_to_rest(data['k'])])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.logger.error('websocket handler error. finish bot')
            self.finished = True

    def _fetch_historical(self, symbol):
        last_timestamp = self.uploader.get_last_timestamp(symbol)
        self.logger.info(f'fetch historical {symbol} last_timestamp {last_timestamp}')
        start_time = (last_timestamp + 1) * 1000
        if self.market_type == MARKET_TYPE_SPOT:
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
            self.uploader.add(symbol, chunk)


def _ws_kline_to_rest(x):
    return [
        x['t'],  # Kline start time
        x['o'],  # ohlcv
        x['h'],
        x['l'],
        x['c'],
        x['v'],
        x['T'],  # Kline close time
        x['q'],  # Quote asset volume
        x['n'],  # Number of trades
        x['V'],  # Taker buy base asset volume
        x['Q'],  # Taker buy quote asset volume
        x['B'],  # Unused field, ignore.
    ]
