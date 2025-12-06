#%%
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path 
from datetime import datetime
from typing import List,Dict, Optional
import logging
import time 
import json

# %%
#configuração de logging
logging.basicConfig(
    level=logging.Info,
    format='%(asctime)s -%(levelname)s - %(message)s'
    
)
logger= logging.getLogger(__name__)

