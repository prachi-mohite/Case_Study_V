
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Case_Study_5_1 {
  def main(args: Array[String]): Unit = {
    println("hey Scala")
    if(args.length!=1)
      {
        System.err.println("Enter the name of file");
        System.exit(1)
      }
    val count = args.length
    println(count)

    //Created spark context
    val conf = new SparkConf().setMaster("local[*]")setAppName("Case_Study_5");
    val sc = new SparkContext(conf);
    println("Spark conext created")
    //Set log level
    sc.setLogLevel("WARN")

    //creating the streaming context
    val ssc = new StreamingContext(sc,Seconds(15))

    //Read the file (which is given as input from command line arguments)
    val filePath = args(0).toString()
    println(filePath)
    val lines = ssc.textFileStream("/home/acadgild/Input/")
    
    //println(lines.count())
    val wordCount = lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}