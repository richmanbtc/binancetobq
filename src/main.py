import os
import time
from .bot import Bot, MARKET_TYPE_SPOT, MARKET_TYPE_PERP
from .uploader import Uploader
from .bq_uploader import BqUploader
from .utils import create_logger, parse_symbols, parse_intervals
from .panic_manager import PanicManager

intervals = parse_intervals(os.getenv('BINANCETOBQ_INTERVALS'))
symbols = parse_symbols(os.getenv('BINANCETOBQ_SYMBOLS'))
market_type = os.getenv('BINANCETOBQ_MARKET_TYPE')
assert(market_type in [MARKET_TYPE_SPOT, MARKET_TYPE_PERP])
project_id = os.getenv('GC_PROJECT_ID')
dataset_name = os.getenv('BINANCETOBQ_DATASET')
log_level = os.getenv('BINANCETOBQ_LOG_LEVEL')
logger = create_logger(log_level)

yappi_enabled = bool(os.getenv('YAPPI_ENABLED', ''))
if yappi_enabled:
    import yappi
    yappi.set_clock_type('cpu')
    yappi.start()
    logger.info('yappi start')

logger.info('start')
logger.info(f'market_type {market_type}')
logger.info(f'intervals {intervals}')
logger.info(f'symbols {symbols}')

panic_manager = PanicManager(logger=create_logger(log_level, 'panic_manager'))
panic_manager.register('bq_uploader', 65 * 60, 65 * 60)

bq_uploader = BqUploader(
    project_id=project_id,
    logger=create_logger(log_level, 'bq_uploader'),
    health_check_ping=lambda: panic_manager.ping('bq_uploader'),
)
uploader = Uploader(
    market_type=market_type,
    intervals=intervals,
    project_id=project_id,
    dataset_name=dataset_name,
    logger=create_logger(log_level, 'uploader'),
    bq_uploader=bq_uploader,
    symbols=symbols,
)
bot = Bot(
    market_type=market_type,
    symbols=symbols,
    logger=create_logger(log_level, 'bot'),
    uploader=uploader,
)

try:
    while not bot.finished:
        time.sleep(0.1)
except KeyboardInterrupt:
    logger.warning('keyboard interrupt')

bot.join()
bq_uploader.join()
panic_manager.join()
logger.info('exit')

if yappi_enabled:
    yappi.get_func_stats().print_all()
    yappi.get_thread_stats().print_all()
