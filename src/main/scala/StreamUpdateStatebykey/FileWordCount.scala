package StreamUpdateStatebykey

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost",8000)
    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey((seq:Seq[Int],option:Option[Int])=>{
      var value = 0
      value += option.getOrElse(0)
      for(elem <- seq){
        value +=elem
      }

      Option(value)
    })
//    val state = result.updateStateByKey[Int](updateFunc = {
//       Option(value)
//})
state.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
