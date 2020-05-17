import Permutation.config
import org.apache.spark
import org.spark_project
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer


object Encoder  extends App {



      //sbt compile
      //sbt run
      //sbt package

      val config: Config = ConfigFactory.load()

      // var stringValueWrite = config.getString("appConfig.sourceDfPath")

      var stringValueRead = config.getString("appConfig.sourceFilePath-Substring")

  var stringValueSource = config.getString("appConfig.sourceFilePath-CompressedDataset")




      //println(stringValue)

      val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParquetTest").
        set("spark.sql.parquet.cacheMetadata","false")
      val sc: SparkContext = new SparkContext(conf)

      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config(conf)
        .getOrCreate()

      //spark.sqlContext.clearCache()
  import spark.implicits._


      var count =0

      var customSchema = StructType(Array(
        StructField("building_id", StringType, true)
      ))



      var df = spark.read.option("inferSchema", "true").parquet(stringValueSource);

  var permutObject = Permutation

  var max1 =permutObject.functionTOGetMaximumNumberOfJoinsNeeded(df)

  println("MaximumNumberOfJoinsNeeded is ",max1)

  var noOfJoinsNeeded = max1-1

  var df2 =spark.read.option("inferSchema", "true").parquet(stringValueRead);

  var df3 = df2

  var columnames = ListBuffer[String]()
  while(count < noOfJoinsNeeded){

    df3 = df3.crossJoin(df2)
    columnames+= config.getString("appConfig.crossJoinColumnName")+count.toString
    count+=1
    }
  columnames+= config.getString("appConfig.crossJoinColumnName")+ count.toString
  var listcolumnnames = columnames.toArray

  import spark.implicits._

  var df4 = df3.toDF(listcolumnnames:_*)

  var subStringColJoinedDF = df4

  var buildingIdDF = df

  var ob = Permutation

 var DecodeDf =  permutObject.Decode(subStringColJoinedDF ,buildingIdDF)

  //var  p= df2.filter ( x => x.get(4).asInstanceOf[String].count(_=='X')==4) to check all strings with 4Xs

  //var id1_checksum = spark.read.parquet("/Users/vaishakhghati/hadoop/parquet-opt/id-checksum")
  //id1_checksum =id1_checksum.select("checkSum").withColumnRenamed("checkSum","checkSum2")

  //val resDf = df2.join(id1_checksum, df2("checkSum") === id1_checksum("checkSum"))

  DecodeDf.repartition(1).write.mode(SaveMode.Append).parquet(config.getString("appConfig.crossJoinedDataFrame"))


  var id_checksum = spark.read.parquet(config.getString("appConfig.id-checksum-from-Encoder-path"))

  id_checksum= id_checksum.withColumn("id",monotonically_increasing_id())

  id_checksum=id_checksum.withColumnRenamed("checkSum","checkSum3")

  val resDf = DecodeDf.join(id_checksum, DecodeDf("checkSum") === id_checksum("checkSum3"),"inner")

  val finalDf = resDf.dropDuplicates("id").select("DecodedString")




  finalDf.repartition(1).write.mode(SaveMode.Append).parquet(config.getString("appConfig.finalOutputPathEncoder"))





}
