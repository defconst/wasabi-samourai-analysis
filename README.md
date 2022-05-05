# Wasabi and Samourai CoinJoin Measurement Study

This repository contains all data and source produced as part of our
measurement study entitled *Measuring the Adoption and Actual Privacy of
Decentralized CoinJoin Implementations in the Bitcoin Ecosystem*.

## Abstract

We present a first measurement study on the adoption and actual privacy of two
popular decentralized CoinJoin implementations, Wasabi and Samourai, in the
broader Bitcoin ecosystem. By applying highly accurate (> 99%) algorithms we
can effectively detect 30,251 Wasabi and 223,597 Samourai transactions within
the block range 530,500 to 725,348 (2018-07-05 to 2022-02-28). We also found a
steady adoption of these services with a total value of mixed coins of ca. 4.74
B USD and average monthly mixing amounts of ca. 172.93 M USD) for Wasabi and
ca. 715.12 M USD for Samourai. Furthermore, we could trace ca. 322 M USD
directly received by cryptoasset exchanges and ca. 1160 M USD indirectly
received via two hops. Our analysis further shows that the traceability of
addresses during the pre-mixing and post-mixing narrows down the anonymity set
provided by these coin mixing services. It also shows that the selection of
addresses for the CoinJoin transaction can harm anonymity. Overall, this is the
first paper to provide a comprehensive picture of the adoption and privacy of
distributed CoinJoin transactions. Understanding this picture is particularly
interesting in the light of ongoing regulatory efforts that will, on the one
hand, affect compliance measures implemented in cryptocurrency exchanges and,
on the other hand, the privacy of end-users.

## Ground-Truth Datasets

* data/wasabi/CoinJoinsMain_20211221.csv: CoinJoin transactions observed by the
  Wasabi coordination server

* data/wasabi/manual_coin_joins.csv: manually executed CoinJoins

## Source Code

### Requirements

* Python 3 (version >= 3.7) 
* R >= 4.2.0
* [GraphSense](https://graphsense.info) instance for Spark jobs
  * Apache Spark Cluster running Spark 3.1.2 / Scala 2.12
  * Apache Cassandra (>= 3.11)
  * DataStax Spark Cassandra connector 3.1.0
