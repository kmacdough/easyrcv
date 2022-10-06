import pandas as pd
import numpy as np
import operator

import logging
LOG = logging.getLogger(__name__)

# Optimizations:
#  - Convert choices to whatever pandas "enum" type is
#  - only update changes in current vote

# Improvements
#  - Consider separating "calculations" from status/operations.
#  - exhausted should be guaranteed to not match a choice.
class CvrStatus:
    def __init__(self, cvr_df, choices_cols, initial_vote_value):
        self.cvr_df = cvr_df
        self.choice_cols = choices_cols
        self.status_df = pd.DataFrame(index=cvr_df.index)
        self.status_df['cur_choice_i'] = 0
        self.status_df['cur_choice'] = cvr_df[choices_cols[0]]
        self.status_df['vote_remaining'] = initial_vote_value

        self.choices_cols = choices_cols

    
    def skip_cvrs_to_next_rank(self, cvr_mask):
        # caching the lookup is probably a premature optimization
        try: self._choice_lookup
        except AttributeError:
            self._choice_lookup = (self.cvr_df[self.choice_cols].join(pd.Series(['exhausted']*len(self.cvr_df), name='exhausted'))).to_numpy()
            self._lookup_range = np.arange(len(self.cvr_df))

        self.status_df.cur_choice_i += cvr_mask
        self.status_df.cur_choice = self._choice_lookup[self._lookup_range, self.status_df.cur_choice_i]

    def skip_cvrs_with_choices(self, choices, repeat=True):
        if not repeat: raise Exception("Not Implemented.")
        while sum(matching_cvrs := self.cur_choice.isin(choices)) > 0:
            LOG.debug(f"Skipping rank on {sum(matching_cvrs)} ballots, {self.count_ballots_by_choice(matching_cvrs).to_dict()}")
            self.skip_cvrs_to_next_rank(matching_cvrs)

    # TODO need better name
    def adjust_votes(self, cvr_mask, ratio):
        self.status_df.loc[cvr_mask, 'vote_remaining'] *= ratio
    

    @property
    def cur_choice(self):
        return self.status_df.cur_choice

    @property
    def exhausted(self):
        return self.status_df.cur_choice == 'exhausted'

    def count_ballots_by_choice(self, cvr_mask):
        return self.status_df.cur_choice[cvr_mask].value_counts()

    def sum_votes_by_choice(self, cvr_mask):
        return self.status_df[cvr_mask].groupby('cur_choice')['vote_remaining'].sum()

    def sum_votes(self, cvr_mask):
        return sum(self.status_df.loc[cvr_mask, 'vote_remaining'])

def tabulate(cvr_df, choice_columns, divide=operator.truediv):
    starting_vote_value = divide(1, 1) # Gives us 1, but of the proper type
    cvr_status = CvrStatus(cvr_df, choice_columns, starting_vote_value)
    num_winners = 2
    winners = []
    losers = []
    round = 0
    while len(winners) < num_winners:
        round += 1
        LOG.info(f"Round {round}. FIGHT!")

        # naively skip overvotes, undervotes, UWIs and current winners/losers
        cvr_status.skip_cvrs_with_choices(['undervote', 'overvote', 'UWI'] + winners + losers, repeat=True)

        threshold = divide(sum(~cvr_status.exhausted), (num_winners - len(winners) + 1))
        LOG.info(f"Threshold: {threshold} votes")
        totals = cvr_status.sum_votes_by_choice(~cvr_status.exhausted)
        if round_winners := totals.index[totals > threshold].to_list():
            LOG.info(f"Ayyoo winners: {round_winners}")
            winners += round_winners
            # do in groupby or somethin
            for winner in round_winners:
                extra_ratio = divide(totals[winner] - threshold, totals[winner])
                is_winner = cvr_status.cur_choice == winner
                cvr_status.adjust_votes(is_winner, extra_ratio)
                cvr_status.skip_cvrs_to_next_rank(is_winner)
                cvr_status.skip_cvrs_with_choices(['undervote', 'overvote', 'UWI'] + winners + losers, repeat=True)
                LOG.info(f"Redistributed {winner}'s {cvr_status.sum_votes(is_winner)} remaining votes: {cvr_status.sum_votes_by_choice(is_winner)}")
        elif round_losers := totals.index[totals.sort_values().cumsum() < threshold].to_list():
            # Batch eliminiation; candidates who can't win, even if transferred ALL votes from worse performers.
            losers += round_losers
            LOG.info(f"Gosh darn losers: {round_losers}")
            for loser in round_losers:
                is_loser = cvr_status.cur_choice == loser
                cvr_status.skip_cvrs_to_next_rank(is_loser)
                cvr_status.skip_cvrs_with_choices(['undervote', 'overvote', 'UWI'] + winners + losers, repeat=True)
                cvr_status.sum_votes_by_choice(is_loser)
                LOG.info(f"Redistributed {loser}'s {cvr_status.sum_votes(is_loser)} remaining votes: {cvr_status.sum_votes_by_choice(is_loser)}")

                



# idx, cols = pd.factorize(df['names'])