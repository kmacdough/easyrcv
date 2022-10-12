import logging
from fractions import Fraction

import numpy as np
import pandas as pd

from pyrcv.config import TabulatorConfig, TabulatorRules

LOG = logging.getLogger(__name__)

# Optimizations:
#  - Convert choices to whatever pandas "enum" type is
#  - only update changes in current vote
#  - categoricals

# Improvements
#  - Consider separating "calculations" from status/operations.
#  - exhausted should be guaranteed to not match a choice.
#  - consider using 1-based indexing. Would take slight coding tweaks, but not unreasonable.
class CvrStatus:
    def __init__(self, cvr_df, choices_cols, initial_vote_value):
        self.cvr_df = cvr_df
        self.choice_cols = choices_cols
        self.df = pd.DataFrame(index=cvr_df.index)
        self.df["_cur_rank_i"] = 0
        self.df["cur_choice"] = cvr_df[choices_cols[0]]
        self.df["vote_remaining"] = initial_vote_value

    @property
    def cur_choice(self):
        """
        Current candidate for each cvr, as set by `self._cur_rank_i`,
        or "exhausted" if self._cur_rank_i > len(self.choice_cols)
        """
        return self.df.cur_choice

    @property
    def vote_remaining(self):
        return self.df.vote_remaining

    @vote_remaining.setter
    def vote_remaining(self, value):
        self.df.vote_remaining = value

    def skip_cvrs_to_next_rank(self, cvr_mask):
        self.df._cur_rank_i += cvr_mask
        self.update_cur_choice_from_rank()

    def skip_cvrs_to_next_rank(self, cvr_mask):
        self.df._cur_rank_i += cvr_mask
        self.update_cur_choice_from_rank()

    def skip_cvrs_with_choices(self, choices, repeat=True):
        if not repeat:
            raise Exception("Not Implemented.")
        while sum(matching_cvrs := self.cur_choice.isin(choices)) > 0:
            LOG.debug(
                f"Skipping rank on {sum(matching_cvrs)} ballots, {self.count_ballots_by_choice(matching_cvrs).to_dict()}"
            )
            self.skip_cvrs_to_next_rank(matching_cvrs)

    # TODO need better name
    def update_vote_remaining(self, for_cvrs, ratio):
        self.df.loc[for_cvrs, "vote_remaining"] *= ratio

    def update_cur_choice_from_rank(self):
        try:  # only need to build lookup once;
            self._choice_lookup
        except AttributeError:
            # numpy array for quickly looking up cvr choice by rank (0-indexed)
            self._choice_lookup = np.hstack(
                [
                    self.cvr_df[self.choice_cols].to_numpy(),
                    np.full((len(self.cvr_df), 1), "exhausted"),
                ],
            )
            self._lookup_range = np.arange(len(self.cvr_df))

        self.df.cur_choice = self._choice_lookup[
            self._lookup_range, self.df._cur_rank_i
        ]

    @property
    def exhausted(self):
        return self.df.cur_choice == "exhausted"

    def count_ballots_by_choice(self, cvr_mask):
        return self.df.cur_choice[cvr_mask].value_counts()

    def sum_votes_by_choice(self, cvr_mask):
        return self.df[cvr_mask].groupby("cur_choice")["vote_remaining"].sum()

    def sum_votes(self, cvr_mask):
        return sum(self.df.loc[cvr_mask, "vote_remaining"])


class BasicStrategy:
    def __init__(self, rules: TabulatorRules):
        self.rules = rules

    def current_threshold(self):
        pass

    def select_winners(self, vote_counts, threshold):
        vote_counts.index[vote_counts > threshold].to_list()


class Tabulator:
    def __init__(self, rules):
        self.rules = rules

    def tabulate(self, cvr_df, choice_columns):
        num_winners = self.rules.number_of_winners
        T = self.rules.T

        cvr_status = CvrStatus(cvr_df, choice_columns, T(1))
        winners = []
        losers = []
        round = 0
        while len(winners) < num_winners:
            round += 1
            LOG.info(f"Round {round}. FIGHT!")

            # naively skip overvotes, undervotes, UWIs and current winners/losers
            cvr_status.skip_cvrs_with_choices(
                ["undervote", "overvote", "UWI"] + winners + losers, repeat=True
            )

            # needs to be vote power

            threshold = self.rules.threshold(
                eligible_votes=cvr_status.sum_votes(~cvr_status.exhausted),
                remaining_winners=num_winners - len(winners),
            )
            LOG.info(f"Threshold: {threshold} votes")
            totals = cvr_status.sum_votes_by_choice(~cvr_status.exhausted)
            if round_winners := self.rules.select_winners(totals, threshold):
                LOG.info(f"Ayyoo winners: {round_winners}")
                winners += round_winners
                # do in groupby or somethin
                for winner in round_winners:
                    excess_vote_ratio = T(
                        T(totals[winner] - threshold) / T(totals[winner])
                    )
                    is_winner = cvr_status.cur_choice == winner
                    cvr_status.vote_remaining.loc[is_winner] = (
                        cvr_status.vote_remaining.loc[is_winner] * excess_vote_ratio
                    )
            elif round_losers := self.rules.select_losers(totals, threshold):
                # Batch eliminiation; candidates who can't win, even if transferred ALL votes from worse performers.
                losers += round_losers
                LOG.info(f"Gosh darn losers: {round_losers}")

            for removed_candidate in round_winners + round_losers:
                chose_candidate = cvr_status.cur_choice == removed_candidate
                cvr_status.skip_cvrs_to_next_rank(chose_candidate)
                cvr_status.skip_cvrs_with_choices(
                    ["undervote", "overvote", "UWI"] + winners + losers, repeat=True
                )
                redistributed = cvr_status.sum_votes_by_choice(chose_candidate)
                LOG.info(
                    f"Redistributed {sum(redistributed)} votes from {removed_candidate}: {redistributed}"
                )
        return {}
