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
from itertools import chain, product
from datetime import datetime, timedelta
import pytz
from math import pi
import time
import statistics

# Plotting
import matplotlib
import matplotlib.pyplot as plt

# Modeling
import sklearn
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import r2_score, classification_report, confusion_matrix, precision_recall_fscore_support, mean_squared_error, accuracy_score, roc_curve, roc_auc_score, auc, make_scorer, precision_score, recall_score, log_loss, f1_score
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
from xgboost import XGBClassifier


# HyperTuning
import optuna
from optuna.samplers import TPESampler

# Save
import pickle
import json
import os
import pathlib