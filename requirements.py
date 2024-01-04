# requirements.py

# Pandas
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
import numpy as np

# Polars (Arrow)
from pyarrow.dataset import dataset
import polars as pl
pl.Config.set_tbl_rows(n=-1)
pl.Config.set_tbl_cols(n=-1)

# Hit API
import requests

# Tools
from itertools import chain
from datetime import datetime, timedelta
import pytz
from math import pi
import time
from itertools import product

# Save
import pickle
import json
import os
import pathlib