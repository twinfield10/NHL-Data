# M6 model vs market

Model prices by season: 20162017: pregame + score matrix, 20172018: pregame + score matrix, 20182019: pregame + score matrix, 20192020: pregame + score matrix, 20202021: pregame + score matrix, 20212022: pregame + score matrix, 20222023: pregame + score matrix, 20232024: pregame + score matrix, 20242025: pregame + score matrix, 20252026: pregame + score matrix. Only pregame prices are honest; `actual (M4)` knows the lineup and starter, which the opening line doesn't. With score matrices every closing line is priced exactly (whole-number totals as P(over | no push)).

## all games

### Information test (closing consensus vs model, pooled)

| market | season | n | b_market | se_market | b_model | se_model | ll_market | ll_model |
|---|---|---|---|---|---|---|---|---|
| moneyline | all | 13184 | 0.5725 | 0.0840 | 0.5272 | 0.0905 | 0.6643 | 0.6649 |
| puckline | all | 13164 | 0.6224 | 0.1011 | 0.3602 | 0.0879 | 0.6480 | 0.6495 |
| total | all | 12776 | 0.5837 | 0.1313 | 0.4663 | 0.0803 | 0.6897 | 0.6907 |

### Out of sample: rolling blend vs the closing market

Positive `gain` means the blend beats the close on later seasons.

| market | n | ll_market | ll_blend | gain |
|---|---|---|---|---|
| moneyline | 11867 | 0.6638 | 0.6629 | 0.0008 |
| puckline | 11848 | 0.6509 | 0.6507 | 0.0001 |
| total | 11624 | 0.6898 | 0.6890 | 0.0008 |

### Betting at the open (¼ Kelly, 2% cap, best non-outlier price)

| market | bets | mean_edge | clv_bets | mean_clv | clv_positive | roi_flat | roi_staked |
|---|---|---|---|---|---|---|---|
| all | 6390 | 0.0588 | 5680 | -0.0021 | 0.4280 | 0.0688 | 0.0796 |
| total | 2721 | 0.0644 | 2026 | -0.0103 | 0.3416 | 0.0650 | 0.0702 |
| moneyline | 3120 | 0.0548 | 3120 | 0.0016 | 0.4609 | 0.0649 | 0.0821 |
| puckline | 549 | 0.0539 | 534 | 0.0070 | 0.5637 | 0.1089 | 0.1307 |

## Nov-Feb

### Information test (closing consensus vs model, pooled)

| market | season | n | b_market | se_market | b_model | se_model | ll_market | ll_model |
|---|---|---|---|---|---|---|---|---|
| moneyline | all | 7433 | 0.4601 | 0.1217 | 0.6512 | 0.1276 | 0.6666 | 0.6659 |
| puckline | all | 7419 | 0.3644 | 0.1434 | 0.6029 | 0.1273 | 0.6459 | 0.6451 |
| total | all | 7207 | 0.4426 | 0.1791 | 0.6171 | 0.1133 | 0.6897 | 0.6887 |

### Out of sample: rolling blend vs the closing market

Positive `gain` means the blend beats the close on later seasons.

| market | n | ll_market | ll_blend | gain |
|---|---|---|---|---|
| moneyline | 6631 | 0.6661 | 0.6647 | 0.0014 |
| puckline | 6618 | 0.6494 | 0.6487 | 0.0007 |
| total | 6513 | 0.6900 | 0.6887 | 0.0013 |

### Betting at the open (¼ Kelly, 2% cap, best non-outlier price)

| market | bets | mean_edge | clv_bets | mean_clv | clv_positive | roi_flat | roi_staked |
|---|---|---|---|---|---|---|---|
| all | 4024 | 0.0627 | 3547 | 0.0015 | 0.4409 | 0.0877 | 0.0927 |
| total | 1796 | 0.0694 | 1344 | -0.0103 | 0.3378 | 0.0652 | 0.0678 |
| moneyline | 1534 | 0.0520 | 1534 | 0.0122 | 0.5117 | 0.1191 | 0.1274 |
| puckline | 694 | 0.0691 | 669 | 0.0005 | 0.4858 | 0.0769 | 0.0905 |
