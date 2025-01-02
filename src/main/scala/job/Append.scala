package job

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object AppendToHistory {

  def main(args: Array[String]): Unit = {
    // Tạo SparkSession
    val spark = SparkSession.builder()
      .appName("Append Daily Data to Historical Data")
      .master("spark://spark-master:7077")
      .getOrCreate()

    // Định dạng ngày để lấy thư mục dữ liệu ngày hôm trước
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val yesterdayDate = LocalDate.now().minusDays(1).format(dateFormatter)

    // Đường dẫn dữ liệu ngày hôm trước
    val dailyPath = s"hdfs://namenode:8020/daily/$yesterdayDate/data.parquet"

    // Đường dẫn lưu trữ dữ liệu lịch sử
    val historicalPath = "hdfs://namenode:8020/raw/all_test.parquet"

    // Đọc dữ liệu từ thư mục ngày hôm trước
    val dailyData = spark.read
      .parquet(dailyPath)

    // Ghi dữ liệu vào tệp lịch sử (append)
    dailyData.write
      .mode(SaveMode.Append)
      .parquet(historicalPath)

    println(s"Appended daily data from $dailyPath to $historicalPath successfully.")

    spark.stop()
  }
}