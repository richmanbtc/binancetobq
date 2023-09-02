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
)
perp_uploader = Uploader(
    market_type=MARKET_TYPE_PERP,
    intervals=['1h', '5m'],
    project_id=project_id,
    dataset_name=dataset_name,
    logger=create_logger(log_level, 'perp_uploader'),
    bq_uploader=bq_uploader,
)
bot = Bot(
    spot_symbols=spot_symbols,
    perp_symbols=perp_symbols,
    logger=create_logger(log_level, 'bot'),
    spot_uploader=spot_uploader,
    perp_uploader=perp_uploader,
)
while not bot.finished:
    time.sleep(0.1)
bot.join()
bq_uploader.join()
logger.info('exit')
