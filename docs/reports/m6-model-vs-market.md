# M6 model vs market

Model prices by season: 20212022: pregame, 20222023: pregame, 20232024: pregame, 20242025: pregame, 20252026: pregame. Only `pregame` prices are honest; `actual (M4)` knows the lineup and starter, which the opening line doesn't.

## Information test (closing consensus vs model, pooled and by season)

| market | season | n | b_market | se_market | b_model | se_model | ll_market | ll_model |
|---|---|---|---|---|---|---|---|---|
| moneyline | all | 6990 | 0.7322 | 0.1086 | 0.3477 | 0.1178 | 0.6585 | 0.6612 |
| puckline | all | 6971 | 0.7065 | 0.1336 | 0.2591 | 0.1129 | 0.6569 | 0.6599 |
| total | all | 6138 | 0.6127 | 0.1998 | 0.4349 | 0.1128 | 0.6892 | 0.6910 |

## Out of sample: rolling blend vs the closing market

Positive `gain` would mean the blend beats the close on later seasons.

| market | n | ll_market | ll_blend | gain |
|---|---|---|---|---|
| moneyline | 5589 | 0.6625 | 0.6630 | -0.0005 |
| puckline | 5570 | 0.6534 | 0.6541 | -0.0007 |
| total | 5068 | 0.6902 | 0.6907 | -0.0005 |

## Betting at the open (¼ Kelly, 2% cap, best non-outlier price)

| market | bets | mean_edge | clv_bets | mean_clv | clv_positive | roi_flat | roi_staked |
|---|---|---|---|---|---|---|---|
| all | 2844 | 0.0552 | 2646 | -0.0019 | 0.4607 | 0.0161 | 0.0237 |
| total | 751 | 0.0552 | 622 | -0.0065 | 0.3585 | -0.0075 | -0.0045 |
| moneyline | 945 | 0.0421 | 945 | 0.0021 | 0.5143 | 0.0235 | 0.0375 |
| puckline | 1148 | 0.0661 | 1079 | -0.0028 | 0.4727 | 0.0255 | 0.0345 |
