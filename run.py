import logging
from fractions import Fraction

import numpy as np
import pandas as pd

from simple_rcv.config import TabulatorConfig
from simple_rcv.tabulate import Tabulator

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

cfg = TabulatorConfig.from_file("2013_minneapolis_park_bottoms_up_config.json")
src_cfg = cfg.cvr_file_sources[0]
df = src_cfg.load_df()

CHOICES = df.columns[src_cfg.first_vote_column_index - 1 :][
    : cfg.rules.max_rankings_allowed
]
df.rename(inplace=True, columns=dict(zip(CHOICES, [c[:3] for c in CHOICES])))
CHOICES = [c[:3] for c in CHOICES]
CANDIDATES = [c.name for c in cfg.candidates]

t = Tabulator(cfg.rules)
t.tabulate(df, CHOICES)