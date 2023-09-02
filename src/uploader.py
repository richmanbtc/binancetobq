import numpy as np
import pandas as pd
from collections import defaultdict
from google.cloud import bigquery


class Uploader:
    def __init__(self, market_type, intervals, project_id,
                 dataset_name, logger, bq_uploader, symbols):
        self.market_type = market_type
        self.df_1m = defaultdict(pd.DataFrame)
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.intervals = intervals
        self.logger = logger
        self.bq_uploader = bq_uploader
        self._initialize_last_timestamps(symbols)

    # old data is ignored
    # rows format: binance kline rest api response
    def add(self, symbol, rows):
        if len(rows) == 0:
            return

        df_1m = self.df_1m[symbol]

        df_new = _rows_to_df(rows)
        if df_1m.shape[0] > 0:
            df_new = df_new.loc[df_new['timestamp'] >= df_1m['timestamp'].max()]
        if df_new.shape[0] == 0:
            return

        df_1m = pd.concat([df_1m, df_new]).reset_index(drop=True)
        df_1m = df_1m.loc[~df_1m['timestamp'].duplicated(keep='last')]  # overwrite
        df_1m = df_1m.sort_values('timestamp', kind='stable')

        if self.df_1m[symbol].shape[0] > 0 and df_1m['timestamp'].max() <= self.df_1m[symbol]['timestamp'].max():
            self.df_1m[symbol] = df_1m
            self.logger.debug(f'skipped because no new timestamp')
            return

        for interval in self.intervals:
            df = df_1m.copy()
            interval_sec = _interval_to_sec(interval)
            df = _process_df(df, interval_sec).reset_index()
            df['symbol'] = symbol

            last_timestamp = self.last_timestamps[interval][symbol]
            df = df.loc[df['timestamp'] > last_timestamp]
            df = df.iloc[:-1]  # remove partial
            df = df.dropna()  # just in case
            if df.shape[0] == 0:
                self.logger.debug(f'skipped because empty df')
                continue

            shape = df.shape
            last_timestamp = df['timestamp'].iloc[-1]
            self.bq_uploader.add(self._get_table_id(interval), df)
            # do not touch df after this line because of thread safety

            self.last_timestamps[interval][symbol] = int(last_timestamp)
            self.logger.info(f'append upload {interval} {symbol} {shape} last_timestamp {last_timestamp}')

        df_1m = df_1m.loc[df_1m['timestamp'] > self.get_last_timestamp(symbol)]
        self.df_1m[symbol] = df_1m

    def get_last_timestamp(self, symbol):
        return min([
            self.last_timestamps[interval][symbol]
            for interval in self.intervals
        ])

    def _initialize_last_timestamps(self, symbols):
        last_timestamps = {}
        for interval in self.intervals:
            table_id = self._get_table_id(interval)

            query = f'SELECT `symbol`, MAX(`timestamp`) as last_timestamp FROM `{table_id}`'
            symbol_in = ','.join([f"'{s}'" for s in symbols])
            cond = [f"`symbol` IN ({symbol_in})"]
            cond = ' AND '.join(cond)
            query += f' WHERE {cond}'
            query += ' GROUP BY `symbol`'
            query_job = self.client.query(query)
            lt = {}
            for row in query_job:
                if row['last_timestamp'] is None:
                    lt[row['symbol']] = 0
                else:
                    lt[row['symbol']] = int(row['last_timestamp'])
            last_timestamps[interval] = lt

        self.last_timestamps = last_timestamps

    def _get_table_id(self, interval):
        table_id = self.dataset_name + '.' + {
            'spot': 'binance_ohlcv_spot',
            'perp': 'binance_ohlcv',
        }[self.market_type]
        table_id += {
            '1h': '',
            '5m': '_5m',
        }[interval]
        return table_id


def _rows_to_df(rows):
    df = pd.DataFrame(rows, columns=[
        'timestamp',
        'op',
        'hi',
        'lo',
        'cl',
        'volume',
        'close_time',
        'amount',
        'trades',
        'buy_volume',
        'buy_amount',
        'ignored',
    ])
    df = df.drop(columns=['close_time', 'ignored'])
    df['timestamp'] = df['timestamp'].astype(int) // 1000
    return df


def _interval_to_sec(x):
    return {
        '1h': 3600,
        '5m': 300,
    }[x]


def _process_df(df, interval_sec):
    for col in ['op', 'hi', 'lo', 'cl', 'volume', 'amount', 'trades', 'buy_volume', 'buy_amount']:
        df[col] = df[col].astype('float')

    df['timestamp_5m'] = (df['timestamp'] // 300) * 300
    df['timestamp'] = (df['timestamp'] // interval_sec) * interval_sec

    if interval_sec > 300:
        df_5m = pd.concat([
            df.groupby('timestamp_5m')['cl'].nth(-1),
        ], axis=1)
        df_5m = df_5m.reset_index()
        df_5m['timestamp'] = (df_5m['timestamp_5m'] // interval_sec) * interval_sec

    df['hi_op'] = df['hi'] - df['op']
    df['lo_op'] = df['lo'] - df['op']

    df['ln_hi_lo'] = np.log(df['hi'] / df['lo'])
    df['ln_hi_lo_sqr'] = df['ln_hi_lo'] ** 2

    cols = [
        df.groupby('timestamp')['op'].nth(0),
        df.groupby('timestamp')['hi'].max(),
        df.groupby('timestamp')['lo'].min(),
        df.groupby('timestamp')['cl'].nth(-1),
        df.groupby('timestamp')['volume'].sum(),
        df.groupby('timestamp')['amount'].sum(),
        df.groupby('timestamp')['trades'].sum(),
        df.groupby('timestamp')['buy_volume'].sum(),
        df.groupby('timestamp')['buy_amount'].sum(),
        df.groupby('timestamp')['cl'].mean().rename('twap'),
        df_5m.groupby('timestamp')['cl'].mean().rename('twap_5m') if interval_sec > 300 else None,
        # vola
        df.groupby('timestamp')['cl'].std().fillna(0).rename('cl_std'),
        df.groupby('timestamp').apply(lambda x: (x['cl'] - x['cl'].shift(1).fillna(df['op'])).std()).fillna(0).rename('cl_diff_std'),
        # slippage
        df.groupby('timestamp')['hi'].mean().rename('hi_twap'),
        df.groupby('timestamp')['lo'].mean().rename('lo_twap'),
        df.groupby('timestamp')['hi_op'].mean().rename('hi_op_max'),
        df.groupby('timestamp')['lo_op'].mean().rename('lo_op_min'),
        # microstructure
        df.groupby('timestamp')['ln_hi_lo'].mean().rename('ln_hi_lo_mean'),
        df.groupby('timestamp')['ln_hi_lo_sqr'].mean().rename('ln_hi_lo_sqr_mean'),
        # df.groupby('timestamp_1h').apply(corwin_alpha).fillna(0).rename('corwin_alpha'),
        # entropy
    ]
    cols = [x for x in cols if x is not None]
    df = pd.concat(cols, axis=1)
    return df
