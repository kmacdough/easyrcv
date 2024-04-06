from __future__ import annotations

import json
import logging
import decimal
import fractions
import enum
import functools
from dataclasses import dataclass
from typing import Any

import pandas as pd

LOG = logging.getLogger(__name__)


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

    def load_df(self, path):
        return pd.read_excel(path / self.file_path)


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


class TiebreakMode(enum.Enum):
    RANDOM = "random"
    USE_CANDIDATE_ORDER = "useCandidateOrder"
    STOP_COUNTING_AND_ASK = "stopCountingAndAsk"
    GENERATE_PERMUTATION = "generatePermutation"
    PREVIOUS_ROUND_COUNTS_THEN_RANDOM = "previousRoundCountsThenRandom"


class OvervoteRule(enum.Enum):
    ALWAYS_SKIP_TO_NEXT_RANK = "alwaysSkipToNextRank"
    EXHAUST_IMMEDIATELY = "exhaustImmediately"
    EXHAUST_IF_MULTIPLE_CONTINUING = "exhaustIfMultipleContinuing"


# fmt: off
class WinnerElectionMode(enum.Enum):
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
            case -1:
                return fractions.Fraction
            case n if n >= 1:
                return decimal.Decimal
                # return float
            case n:
                raise "Expected {n} >= 0 for decimal_places_for_vote_arithmetic"

    def threshold(self, tabulation):
        eligible_votes = tabulation.ballot_set.tally_votes().sum()
        remaining_winners = self.number_of_winners - len(tabulation.winners)
        T = self.arithmetic.T
        if self.hare_quota:
            return self.arithmetic.div(T(eligible_votes), T(remaining_winners))
        else:
            return self.arithmetic.div(T(eligible_votes), (1 + T(remaining_winners)))

    def select_winners(self, round):
        return round.vote_totals.index[
            round.vote_totals > round.vote_threshold
        ].to_list()

    def select_losers(self, round):  # vote_totals, threshold):
        if self.batch_elimination:
            cannot_win = round.vote_totals.sort_values().cumsum() < round.vote_threshold
            return round.vote_totals.index[cannot_win].to_list()
        else:
            return [round.vote_totals.sort_values().index[0]]

    def skip_to_next_eligible_rank(self, ballot_set, ignore_candidates):
        find_ineligible = lambda ballots: ballots.cur_candidate.isin(
            ["undervote", "overvote"] + ignore_candidates
        )
        while 0 < sum(is_ineligible := find_ineligible(ballot_set)):
            msg = f"Skipping rank on {sum(is_ineligible)} ballots, {ballot_set.cur_candidate[is_ineligible].value_counts().to_dict()}"
            LOG.debug(msg)
            ballot_set.skip_to_next_rank(is_ineligible)

    def arithmetic_context(self):
        return decimal.Context(
            prec=4,
            rounding=None,
            Emin=None,
            Emax=None,
            capitals=None,
            clamp=None,
            flags=None,
            traps=None,
        )

    @functools.cached_property
    def arithmetic(self):
        return DecimalArithmetic(self.decimal_places_for_vote_arithmetic)
        match self.decimal_places_for_vote_arithmetic:
            case -1:
                return fractions.Fraction
            case n if n >= 1:
                return decimal.Decimal
                # return float
            case n:
                raise "Expected {n} >= 0 for decimal_places_for_vote_arithmetic"

    @functools.cached_property
    def T(self):
        return arithmetic.type


class DecimalArithmetic:
    def __init__(self, places):
        self.places = places

    @property
    def T(self):
        return decimal.Decimal

    def mul(self, v1, v2):
        (self.T(v1) * self.T(v2)).quantize(self.places, context=self.context)

    def div(self, v1, v2):
        (self.T(v1) / self.T(v2)).quantize(self.places, context=self.context)

    @property
    def context(self):
        return self.Context(rounding=decimal.ROUND_DOWN)
