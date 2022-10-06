from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Any
import json
import pandas as pd

@dataclass
class TabulatorConfig:
    tabulator_version: str
    output_settings: TabulatorOutputConfig
    cvr_file_sources: list[CvrSourceConfig]
    candidates: list[Candidate]
    rules: TabulatorRules

    @classmethod
    def from_file(cls, file):
        with open('2013_minneapolis_park_bottoms_up_config.json', 'r') as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            config_d['tabulatorVersion'],
            TabulatorOutputConfig.from_dict(config_d['outputSettings']),
            [CvrSourceConfig.from_dict(s) for s in config_d['cvrFileSources']],
            [Candidate.from_dict(c) for c in config_d['candidates']],
            TabulatorRules.from_dict(config_d['rules']),
        )

def opt_int(val):
    return None if val == '' else int(val)

def opt_str(val):
    return None if val == '' else str(val)

@dataclass
class TabulatorOutputConfig:
    contest_name: str
    output_directory: str
    contest_date: Any
    contest_jurisdiction: str
    contest_office: str
    tabulate_by_precinct: bool
    generate_cdf_json: bool

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            str(config_d['contestName']),
            str(config_d['outputDirectory']),
            str(config_d['contestDate']),
            str(config_d['contestJurisdiction']),
            str(config_d['contestOffice']),
            bool(config_d['tabulateByPrecinct']),
            bool(config_d['generateCdfJson']),
        )


@dataclass
class CvrSourceConfig:
    file_path: str
    first_vote_column_index: int
    first_vote_row_index: int
    id_column_index: int | None
    precinct_column_index: int
    provider: str
    treat_blank_as_undeclared_write_in: bool
    overvote_label: str
    undervote_label: str
    undeclared_write_in_label: str

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            str(config_d['filePath']),
            int(config_d['firstVoteColumnIndex']),
            int(config_d['firstVoteRowIndex']),
            opt_int(config_d['idColumnIndex']),
            int(config_d['precinctColumnIndex']),
            str(config_d['provider']),
            bool(config_d['treatBlankAsUndeclaredWriteIn']),
            str(config_d['overvoteLabel']),
            str(config_d['undervoteLabel']),
            str(config_d['undeclaredWriteInLabel']),
        )

    def load_df(self):
        return pd.read_excel(self.file_path)


@dataclass
class Candidate():
    name: str
    code: str | None
    excluded: bool

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            str(config_d['name']),
            opt_str(config_d['code']),
            bool(config_d['excluded']),
        )

    def __repr__(self):
        bits = [self.name]
        if self.code: bits.append(f"code={self.code}")
        if self.code: bits.append(f"excluded")
        return "C(" + ', '.join(bits) + ')'

class TiebreakMode(Enum):
    RANDOM = 'random'

class OvervoteRule(Enum):
    ALWAYS_SKIP_TO_NEXT_RANK = 'alwaysSkipToNextRank'

class WinnerElectionMode(Enum):
    BOTTOMS_UP = 'bottomsUp'

@dataclass
class TabulatorRules:
    tiebreak_mode: TiebreakMode
    overvote_rule: OvervoteRule
    winner_election_mode: WinnerElectionMode
    random_seed: int
    number_of_winners: int
    decimal_places_for_vote_arithmetic: int
    minimum_vote_threshold: int
    max_skipped_ranks_allowed: int
    max_rankings_allowed: int
    non_integer_winning_threshold: bool
    hare_quota: bool
    batch_elimination: bool
    exhaust_on_duplicate_candidate: bool
    rules_description: str

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            TiebreakMode(config_d['tiebreakMode']),
            OvervoteRule(config_d['overvoteRule']),
            WinnerElectionMode(config_d['winnerElectionMode']),
            int(config_d['randomSeed']),
            int(config_d['numberOfWinners']),
            int(config_d['decimalPlacesForVoteArithmetic']),
            int(config_d['minimumVoteThreshold']),
            int(config_d['maxSkippedRanksAllowed']),
            int(config_d['maxRankingsAllowed']),
            bool(config_d['nonIntegerWinningThreshold']),
            bool(config_d['hareQuota']),
            bool(config_d['batchElimination']),
            bool(config_d['exhaustOnDuplicateCandidate']),
            str(config_d['rulesDescription']),
        )