import org.apache.spark
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
//import org.apache.spark.graphx.{Graph, VertexId}
//import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}





class TestingRandom {

 // 4a
 def DigitCount(str:String):Int={
   var m = str.count(_ == 'X')
   m
 }

  def functionTOGetMaximumNumberOfJoinsNeeded(df : DataFrame): Int = {
    val DigitCountUDF = udf((str1: String) => DigitCount(str1))
    var df33 = df.withColumn("countofX", DigitCountUDF(col("building_id")))

    var df12 = df33.agg(max("countofX").as("countofXX"))

    var r1 = df12.rdd.collect()

  var r2=  r1.map(x=> x(0).asInstanceOf[Int])

    r2(0)


  }


}
