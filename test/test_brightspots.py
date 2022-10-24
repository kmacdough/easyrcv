import json
from pathlib import Path

import pytest

from easyrcv.config import TabulatorConfig
from easyrcv.tabulate import Tabulator, brightspots_output

BRIGHTSPOTS_RESOURCE_DIR = (
    Path(__file__).parent.resolve()
    / "reference_repos/BrightSpots/rcv/src/test/resources/network/brightspots/rcv/test_data"
)
NON_STANDARD_DIRS = ["unisyn_cvrs", "invalid_params_test", "invalid_sources_test"]
SCENARIO_DIRS = [
    path
    for path in BRIGHTSPOTS_RESOURCE_DIR.iterdir()
    if path.is_dir() and path.name not in NON_STANDARD_DIRS
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

    output = run_scenario(scenario_dir, cfg)
    assert output == expected_summary


def run_scenario(scenario_dir, cfg):
    src_cfg = cfg.cvr_file_sources[0]
    df = src_cfg.load_df(scenario_dir)

    CHOICES = df.columns[src_cfg.first_vote_column_index - 1 :][
        : cfg.rules.max_rankings_allowed
    ]

    t = Tabulator(cfg.rules)
    tabulation = t.tabulate(df, CHOICES)

    x = brightspots_output(tabulation)
    pass
    return x