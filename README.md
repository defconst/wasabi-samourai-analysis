# Wasabi and Samourai CoinJoin Measurement Study

This repository contains all data and source produced as part of our measurement study entitled
*Measuring the Adoption and Actual Privacy of Decentralized CoinJoin Implementations in the Bitcoin Ecosystem*.

## Ground-Truth Datasets

* data/wasabi/CoinJoinsMain_20211221.csv: CoinJoin transactions observed by the Wasabi coordination server

* data/wasabi/manual_coin_joins.csv: manually executed CoinJoins

## Source Code

### Requirements

* Python 3 (version >= 3.7) 
* R >= 4.2.0
* [GraphSense](https://graphsense.info) instance for Spark jobs
  * Apache Spark Cluster running Spark 3.1.2 / Scala 2.12
  * Apache Cassandra (>= 3.11)
  * DataStax Spark Cassandra connector 3.1.0
