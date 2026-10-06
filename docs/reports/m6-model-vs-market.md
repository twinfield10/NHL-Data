# M6 model vs market

Model prices by season: 20162017: pregame + score matrix, 20172018: pregame + score matrix, 20182019: pregame + score matrix, 20192020: pregame + score matrix, 20202021: pregame + score matrix, 20212022: pregame + score matrix, 20222023: pregame + score matrix, 20232024: pregame + score matrix, 20242025: pregame + score matrix, 20252026: pregame + score matrix. Only pregame prices are honest; `actual (M4)` knows the lineup and starter, which the opening line doesn't. With score matrices every closing line is priced exactly (whole-number totals as P(over | no push)).

## all games

### Information test (closing consensus vs model, pooled)

| market | season | n | b_market | se_market | b_model | se_model | ll_market | ll_model |
|---|---|---|---|---|---|---|---|---|
| moneyline | all | 13184 | 0.6512 | 0.0818 | 0.4473 | 0.0906 | 0.6643 | 0.6659 |
| puckline | all | 13164 | 0.7105 | 0.0997 | 0.2779 | 0.0858 | 0.6480 | 0.6505 |
| total | all | 12776 | 0.5842 | 0.1310 | 0.4695 | 0.0804 | 0.6897 | 0.6907 |

### Out of sample: rolling blend vs the closing market

Positive `gain` means the blend beats the close on later seasons.

| market | n | ll_market | ll_blend | gain |
|---|---|---|---|---|
| moneyline | 11867 | 0.6638 | 0.6633 | 0.0005 |
| puckline | 11848 | 0.6509 | 0.6510 | -0.0002 |
| total | 11624 | 0.6898 | 0.6890 | 0.0008 |

### Betting at the open (¼ Kelly, 2% cap, best non-outlier price)

| market | bets | mean_edge | clv_bets | mean_clv | clv_positive | roi_flat | roi_staked |
|---|---|---|---|---|---|---|---|
| all | 5621 | 0.0568 | 4946 | -0.0019 | 0.4218 | 0.0739 | 0.0761 |
| total | 2654 | 0.0638 | 1991 | -0.0103 | 0.3420 | 0.0762 | 0.0768 |
| moneyline | 2636 | 0.0508 | 2636 | 0.0026 | 0.4594 | 0.0651 | 0.0710 |
| puckline | 331 | 0.0489 | 319 | 0.0140 | 0.6082 | 0.1252 | 0.1270 |

## Nov-Feb

### Information test (closing consensus vs model, pooled)

| market | season | n | b_market | se_market | b_model | se_model | ll_market | ll_model |
|---|---|---|---|---|---|---|---|---|
| moneyline | all | 7433 | 0.5266 | 0.1202 | 0.5853 | 0.1281 | 0.6666 | 0.6666 |
| puckline | all | 7419 | 0.4533 | 0.1426 | 0.5182 | 0.1257 | 0.6459 | 0.6458 |
| total | all | 7207 | 0.4487 | 0.1783 | 0.6164 | 0.1128 | 0.6897 | 0.6887 |

### Out of sample: rolling blend vs the closing market

Positive `gain` means the blend beats the close on later seasons.

| market | n | ll_market | ll_blend | gain |
|---|---|---|---|---|
| moneyline | 6631 | 0.6661 | 0.6652 | 0.0009 |
| puckline | 6618 | 0.6494 | 0.6491 | 0.0003 |
| total | 6513 | 0.6900 | 0.6886 | 0.0013 |

### Betting at the open (¼ Kelly, 2% cap, best non-outlier price)

| market | bets | mean_edge | clv_bets | mean_clv | clv_positive | roi_flat | roi_staked |
|---|---|---|---|---|---|---|---|
| all | 3801 | 0.0605 | 3389 | 0.0018 | 0.4429 | 0.0801 | 0.0836 |
| total | 1600 | 0.0672 | 1211 | -0.0095 | 0.3386 | 0.0673 | 0.0669 |
| moneyline | 1621 | 0.0517 | 1621 | 0.0099 | 0.5009 | 0.0945 | 0.1035 |
| puckline | 580 | 0.0663 | 557 | 0.0027 | 0.5009 | 0.0751 | 0.0808 |
