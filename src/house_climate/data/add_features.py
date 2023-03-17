import logging
from pathlib import Path

import pandas as pd
import seaborn as sns
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    format="[%(asctime)s|%(levelname).4s|%(name)s] %(message)s",
    level='DEBUG',
)
logger = logging.getLogger('root')
REPO_ROOT = Path.cwd()
DATA_DIR = REPO_ROOT / "data"
INPUT_PATH = DATA_DIR / "interim" / "preprocessed.parquet"
SAVE_PATH = DATA_DIR / "processed" / "with_features.parquet"


def add_features(df):
    df = (
        df
        .assign(hour_of_day=lambda x: x.index.hour,
                day_of_week=lambda x: x.index.weekday,
                day_of_year=lambda x: x.index.dayofyear,
                )
        .assign(day_night=lambda x: pd.cut(x.hour_of_day, bins=[0,8,20], labels=['night', 'day']),
                is_weekend=lambda x: x.day_of_week.isin([5, 6]),
                )
    )
    return df


if __name__ == "__main__":
    logger.info('STARTING SCRIPT')

    # Load dataset
    logger.info('Loading from disk: %s', INPUT_PATH)
    merged = pd.read_parquet(INPUT_PATH)

    # Add features
    logger.info('Processing new features')
    with_features = merged.pipe(add_features)
    logger.debug('shape: %s', with_features.shape)
    logger.debug('info: %s', with_features.info())
    logger.debug('df:\n%s', with_features)

    # Save to disk
    logger.info('Saving to disk: %s', SAVE_PATH)
    with_features.to_parquet(SAVE_PATH)

    logger.info('ENDING SCRIPT')

