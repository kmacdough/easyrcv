import logging
import math
import decimal
import dataclasses
from typing import Any, Optional

import numpy as np
import pandas as pd

from easyrcv.config import TabulatorConfig, TabulatorRules

LOG = logging.getLogger(__name__)

# Optimizations:
#  - Convert choices to whatever pandas "enum" type is
#  - only update changes in current vote
#  - categoricals


# Improvements
#  - Consider separating "calculations" from status/operations.
#  - exhausted should be guaranteed to not match a choice.
#  - consider using 1-based indexing. Would take slight coding tweaks, but not unreasonable.
class BallotSet:
    def __init__(self, ballot_records, ranks, initial_vote_value):
        self.ballot_records = ballot_records
        self.ranks = ranks
        self.df = pd.DataFrame(index=ballot_records.index)
        self.df["_cur_rank_i"] = 0
        self.df["cur_candidate"] = ballot_records[ranks[0]]
        self.df["vote_remaining"] = initial_vote_value

    @property
    def cur_candidate(self):
        """
        Current candidate for each cvr, as set by `self._cur_rank_i`,
        or "exhausted" if self._cur_rank_i > len(self.ranks)
        """
        return self.df.cur_candidate

    @property
    def vote_remaining(self):
        return self.df.vote_remaining

    def skip_to_next_rank(self, cvr_mask):
        """Move ballots matching `cvr_mask` to next choice"""
        # For each ballot we store a the current
        try:
            self._choice_lookup
        except AttributeError:
            # numpy array for quickly looking up cvr choice by rank (0-indexed)
            self._choice_lookup = np.hstack(
                (
                    self.ballot_records[self.ranks].to_numpy(),
                    np.full((len(self.ballot_records), 1), "exhausted"),
                ),
            )
            self._lookup_range = np.arange(len(self.ballot_records))

        self.df._cur_rank_i += cvr_mask

        self.df.cur_candidate = self._choice_lookup[
            self._lookup_range, self.df._cur_rank_i
        ]

    # TODO need better name
    def update_vote_remaining(self, for_cvrs, ratio):
        self.df.loc[for_cvrs, "vote_remaining"] *= ratio

    def update_cur_candidate_from_rank(self):
        pass

    @property
    def is_exhausted(self):
        return self.df.cur_candidate == "exhausted"

    def count_ballots_by_choice(self, cvr_mask):
        return self.df.cur_candidate[cvr_mask].value_counts()

    def tally_votes(self):
        return (
            self.df[~self.is_exhausted].groupby("cur_candidate")["vote_remaining"].sum()
        )

    def sum_votes(self, cvr_mask):
        return sum(self.df.loc[cvr_mask, "vote_remaining"])


class BasicStrategy:
    def __init__(self, rules: TabulatorRules):
        self.rules = rules

    def current_threshold(self):
        pass

    def select_winners(self, vote_counts, threshold):
        vote_counts.index[vote_counts > threshold].to_list()


@dataclasses.dataclass
class TabulationRound:
    round_number: int
    vote_threshold: Any = None
    vote_totals: Optional[pd.Series] = None
    winners: list[str] = dataclasses.field(default_factory=list)
    losers: list[str] = dataclasses.field(default_factory=list)
    transfers: Optional[pd.Series] = None

    def to_dict(self):
        # move to other classes
        d = {
            "totals": self.totals,
            "winners": self.winners,
            "losers": self.losers,
            "transfers": {
                winner: self.transfers[winner].to_dict()
                for winner in self.transfers.labels[0]
            },
        }


@dataclasses.dataclass
class Tabulation:
    ballot_set: BallotSet
    threshold: Optional[Any] = None
    winners: list[str] = dataclasses.field(default_factory=list)
    losers: list[str] = dataclasses.field(default_factory=list)
    rounds: list[TabulationRound] = dataclasses.field(default_factory=list)


class Tabulator:
    def __init__(self, rules: TabulatorRules):
        self.rules = rules

    def tabulate(self, ballot_records: pd.DataFrame, ranks: list[str]):
        T = self.rules.T
        decimal.getcontext().prec = 4
        decimal.getcontext().rounding = decimal.ROUND_DOWN

        tabulation = Tabulation(BallotSet(ballot_records, ranks, T(1)))

        ignore_candidates = tabulation.winners + tabulation.losers
        self.rules.skip_to_next_eligible_rank(tabulation.ballot_set, ignore_candidates)

        tabulation.threshold = T(math.ceil(self.rules.threshold(tabulation)))

        while len(tabulation.winners) < self.rules.number_of_winners:
            round = self.tabulate_round(tabulation)
            tabulation.winners += round.winners
            tabulation.losers += round.losers
            tabulation.rounds.append(round)

        return tabulation

    def tabulate_round(self, tabulation):
        self.rules.skip_to_next_eligible_rank(
            tabulation.ballot_set, tabulation.winners + tabulation.losers
        )

        round = TabulationRound(1 + len(tabulation.rounds))

        round.vote_threshold = tabulation.threshold
        # round.vote_threshold = self.rules.threshold(tabulation)
        round.vote_totals = tabulation.ballot_set.tally_votes()
        round.winners = self.rules.select_winners(round)
        if round.winners:
            for winner in round.winners:
                T = self.rules.T
                excess_votes = round.vote_totals[winner] - round.vote_threshold
                excess_vote_ratio = T(T(excess_votes) / T(round.vote_totals[winner]))
                picked_winner = tabulation.ballot_set.cur_candidate == winner
                vr = tabulation.ballot_set.vote_remaining
                tabulation.ballot_set.df.loc[
                    picked_winner, "vote_remaining"
                ] *= excess_vote_ratio

            round.transfers = self.transfer_ballots(
                tabulation.ballot_set,
                round.winners,
                ignore_candidates=tabulation.winners + tabulation.losers,
            )
        else:
            round.losers = self.rules.select_losers(round)
            round.transfers = self.transfer_ballots(
                tabulation.ballot_set,
                round.losers,
                ignore_candidates=round.losers + tabulation.winners + tabulation.losers,
            )
        return round

    def transfer_ballots(self, ballot_set, from_candidates, ignore_candidates):
        to_reallocate = ballot_set.cur_candidate.isin(from_candidates)

        before = ballot_set.cur_candidate[to_reallocate]
        ballot_set.skip_to_next_rank(to_reallocate)
        self.rules.skip_to_next_eligible_rank(ballot_set, ignore_candidates)
        after = ballot_set.cur_candidate[to_reallocate]
        votes = ballot_set.vote_remaining[to_reallocate]

        transfers = pd.concat(
            [before, after, votes], keys=["from", "to", "votes"], axis=1
        )
        x = transfers.groupby(["from", "to"])["votes"].sum()
        return x


# TODO doesn't belong here
def brightspots_output(tabulation):
    t = tabulation
    return {
        "config": {
            "contest": "2013 Minneapolis Park Board",
            "date": "",
            "jurisdiction": "Minneapolis",
            "office": "Park and Recreation Commissioner",
            "threshold": "14866",
        },
        "results": [
            brightspots_round_output(i, round)
            for i, round in enumerate(tabulation.rounds)
        ],
    }


def brightspots_round_output(round_index, round: TabulationRound):
    round_num = round_index + 1
    uwi_str = "Undeclared Write-ins"
    winners = [uwi_str if w == "UWI" else w for w in round.winners]
    losers = [uwi_str if w == "UWI" else w for w in round.losers]
    transfers = round.transfers.rename(index={"UWI": uwi_str}).astype(str)
    tally = round.vote_totals.rename(index={"UWI": uwi_str}).astype(str).to_dict()
    tallyResults = [
        {
            "elected": winner,
            "transfers": transfers[winner].to_dict(),
        }
        for winner in winners
    ] + [
        {
            "eliminated": loser,
            "transfers": transfers[loser].to_dict(),
        }
        for loser in losers
    ]
    return {"round": round_num, "tally": tally, "tallyResults": tallyResults}
