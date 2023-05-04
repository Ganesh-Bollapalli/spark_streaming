import org.apache.spark._
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(10))
val filestream = ssc.textFileStream("/projects/gani/*")
filestream.foreachRDD(rdd => {println(rdd.count())})
ssc.start