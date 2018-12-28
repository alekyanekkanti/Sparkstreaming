import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

object Sparkstreaming{
  case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Streaming Word Count").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))



    val bankText = ssc.sparkContext.parallelize(IOUtils.toString(
      new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
      Charset.forName("utf8")).split("\n"))

    val words = bankText.flatMap(_.split(" "))
    //val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
     //wordCounts.print()

    ssc.start()
    ssc.awaitTermination()


  }
}