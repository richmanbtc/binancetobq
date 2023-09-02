

data check

- duplication
- loss
- difference between ws with rest

remove partial

- binance ws publish partial kline

restart

- use docker-compose restart to restart after ws disconnected

reconnect

- https://github.com/sammchardy/python-binance/blob/master/binance/streams.py#L40
- disable auto reconnect and exit if disconnected

python-binance

- start_xxx: single stream per connection
- start_multiplex: multi stream per connection
