import pandas as pd
import numpy as np
from config import TabulatorConfig
from tabulate import tabulate, CvrStatus
from fractions import Fraction
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

cfg = TabulatorConfig.from_file('2013_minneapolis_park_bottoms_up_config.json')
src_cfg = cfg.cvr_file_sources[0]
df = src_cfg.load_df()

CHOICES = df.columns[src_cfg.first_vote_column_index-1:][:cfg.rules.max_rankings_allowed]
df.rename(inplace=True, columns=dict(zip(CHOICES, [c[:3] for c in CHOICES])))
CHOICES =  [c[:3] for c in CHOICES]
CANDIDATES = [c.name for c in cfg.candidates]

tabulate(df, CHOICES, divide=Fraction)