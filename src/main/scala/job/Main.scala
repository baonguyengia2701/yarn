package job

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// ...existing code...

object Main {

  private val checkpointDir = "hdfs://namenode:8020/checkpoint-spark-streaming"
  private val outputDir = "hdfs://namenode:8020/output/parquet"

  def main(args: Array[String]): Unit = {
    val streamingContext = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)

    // Tạo SparkSession
    val spark = SparkSession.builder()
      .config(streamingContext.sparkContext.getConf)
      .getOrCreate()

    import spark.implicits._

    // Cấu hình Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:8097, kafka2:8098, kafka3:8099",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("your_topic_name")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val jsonData = stream.map(record => record.value)

    jsonData.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        val df = spark.read.json(rdd)
        val dfWithDate = df.withColumn("date", current_date())

        dfWithDate.write
          .mode("append")
          .partitionBy("date")
          .parquet(outputDir)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createStreamingContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("KafkaSparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)
    ssc
  }
}