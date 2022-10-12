from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction
from typing import Any

import pandas as pd


@dataclass
class TabulatorConfig:
    tabulator_version: str | None
    output_settings: TabulatorOutputConfig
    cvr_file_sources: list[CvrSourceConfig]
    candidates: list[Candidate]
    rules: TabulatorRules

    @classmethod
    def from_file(cls, file):
        with open(file, "r") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            config_d.get("tabulatorVersion", None),
            TabulatorOutputConfig.from_dict(config_d["outputSettings"]),
            [CvrSourceConfig.from_dict(s) for s in config_d["cvrFileSources"]],
            [Candidate.from_dict(c) for c in config_d["candidates"]],
            TabulatorRules.from_dict(config_d["rules"]),
        )


def opt_int(val):
    return None if val == "" else int(val)


def opt_str(val):
    return None if val == "" else str(val)


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
            str(config_d["contestName"]),
            str(config_d["outputDirectory"]),
            str(config_d["contestDate"]),
            str(config_d["contestJurisdiction"]),
            str(config_d["contestOffice"]),
            bool(config_d["tabulateByPrecinct"]),
            bool(config_d.get("generateCdfJson", False)),
        )


@dataclass
class CvrSourceConfig:
    file_path: str
    first_vote_column_index: int | None
    first_vote_row_index: int
    id_column_index: int | None
    precinct_column_index: int | None
    provider: str
    treat_blank_as_undeclared_write_in: bool
    overvote_label: str
    undervote_label: str
    undeclared_write_in_label: str

    @classmethod
    def from_dict(cls, d):
        return cls(
            str(d["filePath"]),
            opt_int(d.get("firstVoteColumnIndex", "")),
            opt_int(d.get("firstVoteRowIndex", "")),
            opt_int(d.get("idColumnIndex", "")),
            opt_int(d.get("precinctColumnIndex", "")),
            str(d["provider"]),
            bool(d["treatBlankAsUndeclaredWriteIn"]),
            str(d["overvoteLabel"]),
            str(d["undervoteLabel"]),
            str(d["undeclaredWriteInLabel"]),
        )

    def load_df(self):
        return pd.read_excel(self.file_path)


@dataclass
class Candidate:
    name: str
    code: str | None
    excluded: bool

    @classmethod
    def from_dict(cls, config_d):
        return cls(
            str(config_d["name"]),
            opt_str(config_d["code"]),
            bool(config_d["excluded"]),
        )

    def __repr__(self):
        bits = [self.name]
        if self.code:
            bits.append(f"code={self.code}")
        if self.code:
            bits.append(f"excluded")
        return "C(" + ", ".join(bits) + ")"


class TiebreakMode(Enum):
    RANDOM = "random"
    USE_CANDIDATE_ORDER = "useCandidateOrder"
    STOP_COUNTING_AND_ASK = "stopCountingAndAsk"
    GENERATE_PERMUTATION = "generatePermutation"
    PREVIOUS_ROUND_COUNTS_THEN_RANDOM = "previousRoundCountsThenRandom"


class OvervoteRule(Enum):
    ALWAYS_SKIP_TO_NEXT_RANK = "alwaysSkipToNextRank"
    EXHAUST_IMMEDIATELY = "exhaustImmediately"
    EXHAUST_IF_MULTIPLE_CONTINUING = "exhaustIfMultipleContinuing"


# fmt: off
class WinnerElectionMode(Enum):
    BOTTOMS_UP = "bottomsUp"
    SINGLE_WINNER_MAJORITY = "singleWinnerMajority"
    MULTI_WINNER_ALLOW_MULTIPLE_WINNERS_PER_ROUND = "multiWinnerAllowMultipleWinnersPerRound"
    MULTI_WINNER_ALLOW_ONLY_ONE_WINNER_PER_ROUND = "multiWinnerAllowOnlyOneWinnerPerRound"
    MULTI_PASS_IRV = "multiPassIrv"
    BOTTOMS_UP_USING_PERCENTAGE_THRESHOLD = "bottomsUpUsingPercentageThreshold"
# fmt: on

@dataclass
class TabulatorRules:
    tiebreak_mode: TiebreakMode
    overvote_rule: OvervoteRule
    winner_election_mode: WinnerElectionMode
    random_seed: int | None
    number_of_winners: int
    decimal_places_for_vote_arithmetic: int
    minimum_vote_threshold: int | None
    max_skipped_ranks_allowed: int
    max_rankings_allowed: int | None
    non_integer_winning_threshold: bool
    hare_quota: bool
    batch_elimination: bool
    exhaust_on_duplicate_candidate: bool
    rules_description: str

    @classmethod
    def from_dict(cls, d):
        return cls(
            TiebreakMode(d["tiebreakMode"]),
            OvervoteRule(d["overvoteRule"]),
            WinnerElectionMode(d["winnerElectionMode"]),
            opt_int(d.get("randomSeed", "")),
            int(d["numberOfWinners"]),
            int(d["decimalPlacesForVoteArithmetic"]),
            opt_int(d["minimumVoteThreshold"]),
            int(d["maxSkippedRanksAllowed"]),
            None if d["maxRankingsAllowed"] == "max" else int(d["maxRankingsAllowed"]),
            bool(d["nonIntegerWinningThreshold"]),
            bool(d["hareQuota"]),
            bool(d["batchElimination"]),
            bool(d["exhaustOnDuplicateCandidate"]),
            str(d["rulesDescription"]),
        )

    @property
    def T(self):
        match self.decimal_places_for_vote_arithmetic:
            case 0:
                return Fraction
            case n if n >= 1:
                return Fraction
                # return float
            case n:
                raise "Expected {n} >= 0 for decimal_places_for_vote_arithmetic"

    def threshold(self, eligible_votes, remaining_winners):
        if self.hare_quota:
            return self.T(eligible_votes) / (1 + self.T(remaining_winners))
        else:
            return self.T(eligible_votes) / (1 + self.T(remaining_winners))

    def select_winners(self, vote_totals, threshold):
        return vote_totals.index[vote_totals > threshold].to_list()

    def select_losers(self, vote_totals, threshold):
        cannot_win = vote_totals.sort_values().cumsum() < threshold
        return vote_totals.index[cannot_win].to_list()

