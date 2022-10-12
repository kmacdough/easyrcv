import json
from pathlib import Path

import pytest

from pyrcv.config import TabulatorConfig
from pyrcv.tabulate import Tabulator

BRIGHTSPOTS_RESOURCE_DIR = (
    Path(__file__).parent.resolve()
    / "reference_repos/BrightSpots/rcv/src/test/resources/network/brightspots/rcv/test_data"
)
NON_TEST_DIRS = ["unisyn_cvrs"]
SCENARIO_DIRS = [
    path
    for path in BRIGHTSPOTS_RESOURCE_DIR.iterdir()
    if path.is_dir() and path.name not in NON_TEST_DIRS
]


@pytest.mark.parametrize("scenario_dir", SCENARIO_DIRS, ids=lambda dir: dir.name)
def test_read_config(scenario_dir):
    dir_name = scenario_dir.name
    cfg = TabulatorConfig.from_file(scenario_dir / f"{dir_name}_config.json")


@pytest.mark.parametrize("scenario_dir", SCENARIO_DIRS, ids=lambda dir: dir.name)
def test_scenario(scenario_dir):
    dir_name = scenario_dir.name
    cfg = TabulatorConfig.from_file(scenario_dir / f"{dir_name}_config.json")
    with open(scenario_dir / f"{dir_name}_expected_summary.json") as f:
        expected_summary = json.load(f)

    output = run_scenario(cfg)
    assert output == expected_summary


def run_scenario(cfg):
    src_cfg = cfg.cvr_file_sources[0]
    df = src_cfg.load_df()

    CHOICES = df.columns[src_cfg.first_vote_column_index - 1 :][
        : cfg.rules.max_rankings_allowed
    ]

    t = Tabulator(cfg.rules)
    return t.tabulate(df, CHOICES)