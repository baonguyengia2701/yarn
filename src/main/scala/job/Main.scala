package job
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object Main {

  private val checkpointDir = "hdfs://namenode:8020/checkpoint-spark-streaming-reduce"

  def main(args: Array[String]): Unit = {

    val streamingContext = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:8097, kafka2:8098, kafka3:8099",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_off_network_call",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("intra_off_call")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record => record.value)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      if (!rdd.isEmpty()) {
        val hdfsPath = s"hdfs://namenode:8020/intra-network/output_${time.milliseconds}"
        rdd.saveAsTextFile(hdfsPath)
      }
    })
    kafkaStream.foreachRDD { rdd =>
  if (!rdd.isEmpty()) {
    val kafkaDF = rdd.map(record => record.value).toDF("value")

    // Define the schema for JSON data
    val schema = StructType(Array(
      StructField("event_time", TimestampType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", StringType, true),
      StructField("category_id", StringType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", DoubleType, true),
      StructField("user_id", StringType, true),
      StructField("user_session", StringType, true)
    ))

    // Parse the JSON data
    val parsedDF = kafkaDF
      .select(from_json($"value".cast("string"), schema).alias("data"))
      .select("data.*")
      
    // Write to HDFS as partitioned Parquet
    val hdfsPath = "hdfs://namenode:8020/intra-network/parquet_output"
    parsedDF.write
      .mode("append")
      .partitionBy("event_time")
      .parquet(hdfsPath)

    // Read back the newly written data
    val newlyWrittenDF = spark.read.parquet(hdfsPath)

    // Read the existing "master" file
    val masterFilePath = "hdfs://namenode:8020/raw/all.parquet"
    val masterDF = try {
      spark.read.parquet(masterFilePath)
    } catch {
      case _: Throwable => spark.emptyDataFrame // If the master file doesn't exist, start with empty DataFrame
    }

    // Append the new data to the master file
    val updatedMasterDF = masterDF.union(newlyWrittenDF)

    // Write back the combined data to the master file
    updatedMasterDF.write
      .mode("overwrite")
      .parquet(masterFilePath)
  }
}


    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def createStreamingContext(): StreamingContext = {
    val sparkConfig = new SparkConf().setMaster("spark://spark-master:7077").setAppName("Intra-network stream")
    val streamingContext = new StreamingContext(sparkConfig, Seconds(30))
    streamingContext.checkpoint(checkpointDir)
    streamingContext
  }
}