import logging
import os

log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', '..', 'log')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    filename=os.path.join(log_dir, 'etl.log'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
