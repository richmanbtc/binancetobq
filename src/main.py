import os
import time
from .bot import Bot, MARKET_TYPE_SPOT, MARKET_TYPE_PERP
from .uploader import Uploader
from .bq_uploader import BqUploader
from .utils import create_logger, parse_symbols
from .panic_manager import PanicManager

spot_symbols = parse_symbols(os.getenv('BINANCETOBQ_SPOT_SYMBOLS'))
perp_symbols = parse_symbols(os.getenv('BINANCETOBQ_PERP_SYMBOLS'))
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
logger.info(f'spot_symbols {spot_symbols}')
logger.info(f'perp_symbols {perp_symbols}')

panic_manager = PanicManager(logger=create_logger(log_level, 'panic_manager'))
panic_manager.register('bq_uploader', 15 * 60, 15 * 60)

bq_uploader = BqUploader(
    project_id=project_id,
    logger=create_logger(log_level, 'bq_uploader'),
    health_check_ping=lambda: panic_manager.ping('bq_uploader'),
)
spot_uploader = Uploader(
    market_type=MARKET_TYPE_SPOT,
    intervals=['1h'],
    project_id=project_id,
    dataset_name=dataset_name,
    logger=create_logger(log_level, 'spot_uploader'),
    bq_uploader=bq_uploader,
    symbols=spot_symbols,
)
perp_uploader = Uploader(
    market_type=MARKET_TYPE_PERP,
    intervals=['1h', '5m'],
    project_id=project_id,
    dataset_name=dataset_name,
    logger=create_logger(log_level, 'perp_uploader'),
    bq_uploader=bq_uploader,
    symbols=perp_symbols,
)
bot = Bot(
    spot_symbols=spot_symbols,
    perp_symbols=perp_symbols,
    logger=create_logger(log_level, 'bot'),
    spot_uploader=spot_uploader,
    perp_uploader=perp_uploader,
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
