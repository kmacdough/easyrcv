import logging
import os
import sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pandas as pd

from easyrcv.config import TabulatorConfig
from easyrcv.tabulate import Tabulator

logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)

if sys.argv[1] and Path(sys.argv[1]).is_file():
    config_file = Path(sys.argv[1]).absolute()
else:
    config_file = Path(os.getcwd()) / "2013_minneapolis_park_bottoms_up_config.json"
cfg = TabulatorConfig.from_file(config_file)
src_cfg = cfg.cvr_file_sources[0]
df = src_cfg.load_df(src_cfg.cvr_file_sources)

CHOICES = df.columns[src_cfg.first_vote_column_index - 1 :][
    : cfg.rules.max_rankings_allowed
]
df.rename(inplace=True, columns=dict(zip(CHOICES, [c[:3] for c in CHOICES])))
CHOICES = [c[:3] for c in CHOICES]
CANDIDATES = [c.name for c in cfg.candidates]

t = Tabulator(cfg.rules)
t.tabulate(df, CHOICES)