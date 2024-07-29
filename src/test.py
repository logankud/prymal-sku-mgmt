from datetime import datetime
import pandas as pd
from loguru import logger

# value = pd.to_datetime('2024-05-05T23:36:07.260134100+00:00')
value = '2024-05-05T23:36:07.260134100+00:00'

# val_iso = pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%S%z')

if isinstance(value, datetime):
  # Format timestamp
  logger.info(value.strftime('%Y-%m-%dT%H:%M:%S%z'))
elif isinstance(value, str):
  try:
    # Attempt to parse the string to a datetime object
    logger.info(pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%S%z'))
  except ValueError:
    raise ValueError(
        f"Field must be in ISO 8601 format if provided as a string")

# raise ValueError(
#     f"Field must be a datetime object or a string in ISO 8601 format"
# )

# logger.info(value)
# logger.info(val_iso)
