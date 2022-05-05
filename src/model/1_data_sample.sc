import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame

val hexStringToByteArray = udf((x: String) => x.grouped(2).toArray map { Integer.parseInt(_, 16).toByte })

def loadCassandraTable(keyspace: String, table: String) = {
  spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> keyspace, "table" -> table))
    .load
}

def storeParquet(df: DataFrame, path: String) = {
  df.write.mode("overwrite").parquet(path)
}

val hdfsPath = "hdfs://spark-master/user/rcs/wasabi"
val keyspaceRaw = "btc_raw_nts"

val tx = loadCassandraTable(keyspaceRaw, "transaction")

val manualTxsWasabi = (spark.read.format("csv")
  .option("header", true)
  .load(f"$hdfsPath/manual_coin_joins.csv")
  .filter($"block" >= 718608) // remove manual tx in block 627865 (included in CoinJoinsMain_20211221.txt)
  .select(hexStringToByteArray($"tx_hash").as("tx_hash"))
  .join(tx, Seq("tx_hash"), "left")
  .persist())

val wasabiTxHashes = (spark.read.format("csv")
  .option("header", true)
  .load(f"$hdfsPath/CoinJoinsMain_20211221.csv")
  .select(hexStringToByteArray($"tx_hash").as("tx_hash")))

val filteredTxsWasabi = (tx
  .join(wasabiTxHashes, Seq("tx_hash"), "right")
  .persist())

val minBlock: Long = filteredTxsWasabi.select(floor(min($"block_id") / 100) * 100).first.getLong(0)
val maxBlock: Long = filteredTxsWasabi.select(ceil(max($"block_id") / 100) * 100).first.getLong(0)

val filteredTxsNonWasabi = (tx
  .filter($"block_id" >= minBlock && $"block_id" < maxBlock)
  .join(wasabiTxHashes, Seq("tx_hash"), "left_anti")
  .sort($"tx_id".asc)
  .persist())

// sample balanced negative sample for model
val frac = wasabiTxHashes.count.toDouble / filteredTxsNonWasabi.count
val nonWasabiTxIdSample = filteredTxsNonWasabi.sample(frac, 2022).select("tx_id")
val sampledTxsNonWasabi = (filteredTxsNonWasabi
  .join(nonWasabiTxIdSample, Seq("tx_id"), "right")
  .persist())

// negative samples for validation
val remainingNonWasabiTxIds = (filteredTxsNonWasabi
  .join(sampledTxsNonWasabi, Seq("tx_id"), "left_anti")
  .sort($"tx_id".asc))
val sampledTxsNonWasabiValidation = (filteredTxsNonWasabi
  .join(
    remainingNonWasabiTxIds
      .sample(100000 / remainingNonWasabiTxIds.count.toDouble, 2022)
      .select("tx_id"),
    Seq("tx_id"), "right")
  .persist())

manualTxsWasabi.count
filteredTxsWasabi.count
sampledTxsNonWasabi.count
sampledTxsNonWasabiValidation.count

val txSample = (sampledTxsNonWasabi
  .withColumn("type", lit("non_wasabi"))
  .unionByName(sampledTxsNonWasabiValidation.withColumn("type", lit("non_wasabi_validation")))
  .unionByName(filteredTxsWasabi.withColumn("type", lit("wasabi")))
  .unionByName(manualTxsWasabi.withColumn("type", lit("wasabi_manual")))
  .withColumn("tx_hash", lower(hex($"tx_hash"))))

storeParquet(txSample, f"$hdfsPath/tx_sample.parquet")
