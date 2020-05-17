import org.apache.spark
import org.spark_project
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}





object Main1 {

  def main(args: Array[String]): Unit = {

    //sbt compile
    //sbt run
    //sbt package

    val config: Config = ConfigFactory.load()

    // var stringValueWrite = config.getString("appConfig.sourceDfPath")

    var stringValueRead = config.getString("appConfig.sourceFilePath")




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



    var df = spark.read
      .schema(customSchema)
      .option("inferSchema", "true")
      .parquet(stringValueRead);





    var obj = new Join();

    var df2 = obj.returnWithId(df)

    df2 = obj.returnWithAllSmallAndCheckSum(df2)

    println(df2.show(5, false))

    // var dfBuildingId = df2.select("building_id")


    var df3 = obj.CreateDataFrameWithSubStringsAndLengths(df2)

    var res50= df3.join(df2, df2("building_id") contains df3("subStringCol"),"outer")

  var res67=   res50.filter("subStringColId is not null")

    var res68 = res67.filter("id is not null")
    var new1= res68.select("id","subStringColId")

    new1 = new1.groupBy("id").agg(collect_list("subStringColId").as("DigitCount"))
    new1.repartition(1).write.mode(SaveMode.Append).parquet("/Users/vaishakhghati/Desktop/pauper-parquet1/")


    var Compresseddf2 = obj.compress(df2, df3)

    var obtainId = Compresseddf2.filter(col("compressed") rlike ".*"+ config.getString("appConfig.replaceSubStringWithChar")+".*")

    var finalSave1 = Compresseddf2.select("compressed").withColumnRenamed(
      "compressed",
      "building_id"
    );

    var finalSave2 = obtainId.select("id", "checkSum")

   // finalSave2 = Compresseddf2.select("id", "checkSum")  Not saving id to savespace. Only if
    //you are saving Compresseddf2, save without the id, if you are using obtainId (filtered set of Ids that have 'X' in them)
    //Save withID.

    finalSave2 = Compresseddf2.select( "checkSum")

     finalSave2.repartition(1).write.mode(SaveMode.Append).parquet(config.getString("appConfig.finalSave2"))







    finalSave1.repartition(1).write.mode(SaveMode.Append).parquet(config.getString("appConfig.destinationFilePath"))

    var finalSave3 = df3.select("subStringCol")

    finalSave3.cache()

    println(finalSave3.count)

    customSchema = StructType(Array(
      StructField("subStringCol", StringType, true)
    ))

    // var stringValueRead2 = config.getString("appConfig.sourceFilePathForFile3")

    /* var fin=spark.read
       .schema(customSchema)
       .option("inferSchema", "true").
       parquet("once3")

     fin.cache()

     println(fin.count)

    var finalSave4=  finalSave3.union(fin)*/

    var finalSave4=  finalSave3



    finalSave4.repartition(1).write.mode(SaveMode.Append).parquet(config.getString("appConfig.finalSave3"))


    count+=1












    println()
    println()
    //println(df3.show(5))

    println()
    println()
    println()
    println()
    println()



























  }

}
