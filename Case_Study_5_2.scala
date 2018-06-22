import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Level, Logger}
import scala.io.Source._

object Case_Study_5_2 {
  def main(args: Array[String]): Unit = {
    println("hey Scala")
    if (args.length != 2) {
      System.err.println("Usage Case_Study_5_2<LocalDirectory> <HDFSDirectory>");
      System.exit(1)
    }

    val localFilePath = args(0).toString() 
    val dfsDirPath = args(0).toString()

    //println("HDFSWordCountComparison : Main Called Successfully")

    println("Performing local word count")
    val fileContents = readFile(localFilePath.toString()+"test1.txt")

    println("Performing local word count - File Content ->>" + fileContents)
    val localWordCount = RunLocalWordCount(fileContents)

    println("SparkHDFSWordCountComparison : Main Called Successfully -> Local Word Count is ->>" + localWordCount)
    println("Performing local word count Completed !!")

    println("Creating Spark Context")
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkHDFSWordCountComparisonApp")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    println("Spark Context Created")

    println("Writing local file to DFS")
    val dfsFilename = dfsDirPath + "/local_to_hdfs"
    val fileRDD = sc.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFilename)
    println("Writing local file to DFS Completed")

    println("Reading file from DFS and running Word Count")
    val readFileRDD = sc.textFile(dfsFilename)

    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

    sc.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) are same.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) are not same.")
    }
  }

  private def RunLocalWordCount(fileContent:List[String]):Int={
    var wordCount = fileContent.flatMap(_.split(" ")).filter(_.nonEmpty).groupBy(w=>w).mapValues(_.size).values.sum;
    return wordCount;
  }

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

}

