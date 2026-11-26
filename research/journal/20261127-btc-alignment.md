Exploratory replay of all 142 TRADE rows from job 140218404875. Months already examined. No independent holdout claim. BTC alignment requires side * btc_trend > 0.

Month | aligned trades | wins | net R | stress R
---|---:|---:|---:|---:
May|3|0|-3.90|-4.35
June|1|0|-1.23|-1.35
July|1|1|0.97|0.86
August|23|13|0.75|-2.13
September|1|1|0.90|0.75
October|0|0|0|0
November|7|6|4.56|3.73

GradientBoostingClassifier(30 depth-one trees, learning_rate .05, seed 17), sklearn 1.8.0. Features score, side, btc_volatility, btc_trend, btc_ret12, m5_atr, m5_relvol, m15_flow3, h1_trend. Positive label net > 0. Last prior available month for sigmoid calibration with default LogisticRegression on decision_function, earlier history for fitting, prior exits strictly before next block first entry. Each training/calibration block requires 10 rows and both classes. Probability cutoff .60. July and September accepted zero, November accepted all 30, 7 wins, -19.82R (-22.43R stress). Other months insufficient training/calibration history. Neither method satisfied the monthly target. Future comparison needs broader candidate events and untouched dates.