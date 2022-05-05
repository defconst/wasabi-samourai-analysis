import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

def loadCassandraTable(keyspace: String, table: String) = {
  spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> keyspace, "table" -> table))
    .load
}

def storeParquet(df: DataFrame, path: String) = {
  df.write.mode("overwrite").parquet(path)
}

val hdfsPath = "hdfs://spark-master/user/rcs/wasabi"
val keyspace = "btc_transformed_20220212"

val txSample = spark.read.parquet(f"$hdfsPath/tx_sample.parquet")
val address = loadCassandraTable(keyspace, "address").select("address", "cluster_id")
val cluster = loadCassandraTable(keyspace, "cluster").select("cluster_id", "no_addresses")

val inputStats = (txSample
  .select($"tx_hash", explode($"inputs.value").as("value"))
  .groupBy($"tx_hash")
  .agg(count($"value").as("num_input_val"))
  .na.fill(0)) // coinbase txs

val outputStats = (txSample
  .select($"tx_hash", explode($"outputs.value").as("value"))
  .withColumn("dec_places", length(regexp_replace(substring(lpad($"value", 16, "0"), 9, 8), "0+$", "")))
  .groupBy($"tx_hash")
  .agg(
    min($"value").as("min_output_val"),
    max($"value").as("max_output_val"),
    mean($"value").as("mean_output_val"),
    count($"value").as("num_output_val"),
    countDistinct($"value").as("num_distinct_output_val"),
    mean($"dec_places").as("mean_dec_places")))

// number of unique output values
val numUniqueOutputVal = (txSample
  .select($"tx_hash", explode($"outputs.value").as("value"))
  .groupBy($"tx_hash", $"value")
  .count()
  .filter($"count" === 1)
  .groupBy($"tx_hash")
  .count()
  .withColumnRenamed("count", "num_uniq_output_val")
  .join(txSample.select("tx_hash"), Seq("tx_hash"), "right")
  .na.fill(0))

// number of equal output values
val numEqualOutputVal = (txSample
  .select($"tx_hash", explode($"outputs.value").as("value"))
  .groupBy($"tx_hash", $"value")
  .count()
  .filter($"count" > 1)
  .groupBy($"tx_hash")
  .agg(sum($"count").as("num_eq_output_val"))
  .join(txSample.select("tx_hash"), Seq("tx_hash"), "right")
  .na.fill(0))

// occurence of most frequent output value
val window = Window.partitionBy($"tx_hash").orderBy($"count".desc)
val numMaxFreqValue = (txSample
  .select($"tx_hash", explode($"outputs.value").as("value"))
  .groupBy($"tx_hash", $"value")
  .count()
  .withColumn("order", row_number().over(window))
  .filter($"order" === 1)
  .select($"tx_hash", $"value".as("max_freq_output_val"), $"count".as("num_max_freq_output_val")))

// number of input addresses reused as outputs in the same tx
val inputAddrByTxHash = (txSample
  .select($"tx_hash", explode($"inputs.address").as("address"))
  .select($"tx_hash", explode($"address").as("address")))
val outputAddrByTxHash = (txSample
  .select($"tx_hash", explode($"outputs.address").as("address"))
  .select($"tx_hash", explode($"address").as("address")))
val numInputReuse = (inputAddrByTxHash
  .join(outputAddrByTxHash, Seq("tx_hash", "address"), "inner")
  .distinct
  .groupBy("tx_hash")
  .count()
  .withColumnRenamed("count", "num_input_reuse")
  .join(txSample.select("tx_hash"), Seq("tx_hash"), "right")
  .na.fill(0))

// average size (numner of addresses) of the tx output address clusters
val meanOutputClusterSize = (txSample
  .select($"tx_hash", posexplode($"outputs.address").as(Seq("n", "address")))
  .select($"tx_hash", $"n", explode($"address").as("address"))
  .join(address, Seq("address"), "left")
  .join(cluster, Seq("cluster_id"), "left")
  .groupBy("tx_hash")
  .agg(mean($"no_addresses").as("mean_output_cluster_size"))
  .join(txSample.select("tx_hash"), Seq("tx_hash"), "right")
  .na.fill(0) // null if output is a single non-standard output
  .persist())

val nativeSegWit = (txSample
  .select(
    $"tx_hash",
    array_distinct($"inputs.address_type").as("input_address_types"),
    array_distinct($"outputs.address_type").as("output_address_types"))
  .withColumn(
    "all_inputs_segwit",
    array_contains($"input_address_types", 8) && size($"input_address_types") === 1)
  .withColumn(
    "all_outputs_segwit",
    array_contains($"output_address_types", 8) && size($"output_address_types") === 1)
  .withColumn("is_native_segwit", $"all_inputs_segwit" && $"all_outputs_segwit")
  .drop("input_address_types", "output_address_types"))

// merge final dataframe
val res = (txSample
  .select("tx_hash", "tx_id", "type")
  .join(inputStats, Seq("tx_hash"), "left")
  .join(outputStats, Seq("tx_hash"), "left")
  .join(numUniqueOutputVal, Seq("tx_hash"), "left")
  .join(numEqualOutputVal, Seq("tx_hash"), "left")
  .join(numMaxFreqValue.drop("max_freq_output_val"), Seq("tx_hash"), "left")
  .join(numInputReuse, Seq("tx_hash"), "left")
  .join(meanOutputClusterSize, Seq("tx_hash"), "left")
  .join(nativeSegWit, Seq("tx_hash"), "left")
  .withColumn("ratio_num_input_num_output", $"num_input_val" / $"num_output_val")
  .withColumn("ratio_num_input_num_max_freq_output", $"num_input_val" / $"num_max_freq_output_val"))

storeParquet(res, f"$hdfsPath/extracted_features.parquet")

// Ficsor heuristic
val txSampleFicsor = (txSample
  .select(
    $"tx_id",
    $"tx_hash",
    $"type",
    size($"inputs").as("num_inputs"),
    size(array_distinct($"outputs.value")).as("num_distinct_output_val"),
    (array_contains(flatten($"outputs.address"), "bc1qs604c7jv6amk4cxqlnvuxv26hv3e48cds4m0ew") ||
      array_contains(flatten($"outputs.address"), "bc1qa24tsgchvuxsaccp8vrnkfd85hrcpafg20kmjw"))
      .as("has_coordinator_output"))
  .join(numUniqueOutputVal, Seq("tx_hash"), "left")
  .join(numMaxFreqValue, Seq("tx_hash"), "left")
  .join(nativeSegWit.drop("all_inputs_segwit", "all_outputs_segwit"), Seq("tx_hash"), "left")
  .withColumn(
    "is_wasabi_coinjoin_static",
    $"has_coordinator_output" && ($"num_max_freq_output_val" > 2))
  .withColumn(
    "is_wasabi_coinjoin_heuristic_old",
    ($"num_inputs" >= $"num_max_freq_output_val") &&
      ($"num_max_freq_output_val" >= 10) &&
      ($"num_uniq_output_val") >= 2 &&
      (abs($"max_freq_output_val" - 0.1 * 1e8) <= 0.02 * 1e8))
  .withColumn(
    "is_wasabi_coinjoin_heuristic",
    $"is_wasabi_coinjoin_heuristic_old" && $"is_native_segwit"))

storeParquet(txSampleFicsor, f"$hdfsPath/tx_sample_ficsor.parquet")
