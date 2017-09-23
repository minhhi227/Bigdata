

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object ApacheLogFileAnalytics {

  def main(arg1: Array[String]): Unit = {

    //spark configuration
    val sparkConf = new SparkConf().setAppName("Log Analyze").setMaster("local[2]").set("spark.executor.memory", "1g");
    //spark context
    val sc = new SparkContext(sparkConf)

    val filename = arg1(0)

    //RDD Access Logs
    val accessFile = sc.textFile(filename).map(LogParser.parseLog).cache()

    println("******START PROCESS ANALYZER********")


    //response code
    val responseCode = accessFile.map(log => (log.responseCode, 1))
                      .reduceByKey(_ + _)
                      .take(100)

    println("======COMPUTE FREQUENCIES=============")
    println(s"""Error Resposnes: \n${responseCode.mkString("\n")}""")
    println();

    //ipaddress
    val ipAccess = accessFile.map(log => (log.ipAddress, 1))
                             .reduceByKey(_ + _)
                             .filter(_._2 > 21)
                             .take(100)

    println("======ACCESS MORE THAN================")
    println(s"""${ipAccess.mkString("\n")}""")
    println()

    //contents access
    val contentAccess = accessFile.map(log => (log.endpoint, 1))
                                     .reduceByKey(_ + _)
                                     .filter(_._2 > 21)
                                     .take(100)

    println("=======ENDPOINT REQUEST===========")
    println(s"""${contentAccess.mkString("\n")}""")
    println()
    
    //contents access sort
    val topContentAccess = accessFile.map(log => (log.endpoint, 1))
                                     .reduceByKey(_ + _)
                                     .top(10)(Ordering.OrderValue)

    println("=======TOP ENDPOINT REQUEST===========")
    println(s"""${topContentAccess.mkString("\n")}""")
    println()

    //content size
    val contentSizes = accessFile.map(log => log.contentSize)
    println("=======AVERAGE SIZE============")
    println("Size of Avg:", contentSizes.reduce(_ + _) / contentSizes.count)
    println()

    println("******STOP PROCESS*****************")

    sc.stop()

  }

}

object Ordering {
  object OrderValue extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2 compare b._2
    }
  }
}