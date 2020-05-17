
import com.typesafe.config.{Config, ConfigFactory}
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
import com.roundeights.hasher.Implicits._

class Join extends  Serializable {

  val config: Config = ConfigFactory.load()


  def getMd5CheckSum(str : String):Int ={
    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"
    var totalSum=0;
    // str.foreach()
    var count1=1
    str.foreach( i => {
      totalSum+=(allCharsString.indexOf(i)*2*count1)
      count1+=1
    })
    totalSum
  }

  def DigitCount(str:String):String={

    return str.length.toString
  }

  def   getChekSum(str : String) : Int  ={

   // val result = "Nidhi".indexOf('h')

    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"

    var totalSum=0;
   // str.foreach()
    str.foreach( i => {


      //  println(i)

        totalSum+=(allCharsString.indexOf(i)*2)

    //  println(totalSum)




    })
    totalSum=getMd5CheckSum(str.sha256)
    totalSum
   // str.sha256
  }



  def   returnWithAllSmallAndCheckSum(df:DataFrame) : DataFrame  ={

    val lowerCaseUDF = udf((str1:String) =>  str1.toLowerCase)
  //  var rdd6DF2=rdd6DF.withColumn("DigitCount",DigitCountUDF(rdd6DF("subStringCol") ) )

   // var d1 =df.withColumn("building_id",lowerCaseUDF(col("building_id")))
    val ChekSumUDF = udf((str1:String) => getChekSum(str1))
   var d2= df.withColumn("checkSum", ChekSumUDF(col("building_id")))
    d2
  }

  def   returnWithId(df:DataFrame) : DataFrame  ={

    var d1 =df.withColumn("id",monotonically_increasing_id())
    d1
  }



  def   compress(df2:DataFrame,df3:DataFrame) : DataFrame  ={

    var  df3RDD = df3.rdd.map(x => x.get(0).asInstanceOf[String] )

    var  df3RDD2 = df3.rdd.map(x => x.get(2).asInstanceOf[Long] )

    var rdd1= df3RDD.collect()

    var rdd2= df3RDD2.collect()

    def Compress(str: String,id1:Long):String ={

     /* var rdd1= Array("4a"," 9b"," 4b"," de"," d4"," 91"," 27"," 1b"," 83",
        " 69"," a6"," ba"," 49"," bc"," 5e"," af"," 4d"," f6"," 8b",
        " 8e"," d2"," 9e"," 3f"," 1a"," 7b"," 2e"," 6d"," 50"," 8c",
        " 75"," ab"," d6"," a1"," c2"," 12"," 3a"," 0e"," 42"," e7",
        " 2a"," 82"," c8"," 04"," b8"," aa"," 9d"," 95"," 29"," 97",
        " c4"," d5"," 85"," 20"," 54"," 63"," 89"," 72"," 0c"," 13",
        " c5"," 22"," 60"," 02"," a4"," 6b"," 3c"," 61"," 10"," b2"," 78"," 3e"," 1c"," fc"," 0f"," 32"," fa"," 7a"," 87"," a3"," dd"," 41"," b1"," 07"," ca"," 4e"," bf"," d3"," 5a"," 23"," d9"," 53"," 44"," 18"," 93"," 01"," e8"," 68"," 9c"," 7e","bb")
*/
      var newString = str
            rdd1.indices.foreach(i => {
        if(newString.contains(rdd1(i))){
          newString= newString.replaceAll(rdd1(i), config.getString("appConfig.replaceSubStringWithChar"))
          println(newString)
        }
      })
      newString
    }
    val compressUDF = udf((str1:String,id:Long) => Compress(str1,id))
    var d2= df2.withColumn("compressed", compressUDF(col("building_id"),col("id")))
    d2
  }

  def CreateDataFrameWithSubStringsAndLengths(df:DataFrame) : DataFrame  ={

    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._

    val toArray = udf((b: String) =>  b.grouped(4).toList ++ b.grouped(2).toList ++ b.grouped(3).toList)

    val df2 = df.withColumn("b", toArray(col("building_id")))

    var df3=df2


    var rdd1 = df3.rdd

    var rdd2 =  rdd1.map(x => x.get(0).asInstanceOf[String])

    var rdd4 = rdd2.map(x => {
      var p=x.grouped(2).toList
      var p2=x.grouped(3).toList
      var p4=x.grouped(4).toList

      var p31 = p++p2++p4
      p31
    })

    var rdd5 = rdd4.flatMap(x => x)

  //  var rdd6 = rdd5.filter(f => f.length!=1)
  var rdd6 = rdd5

   var rdd6DF= rdd6.toDF("subStringCol")

    val DigitCountUDF = udf((str1:String) => DigitCount(str1))
   var rdd6DF2=rdd6DF.withColumn("DigitCount",DigitCountUDF(rdd6DF("subStringCol") ) )

   var  rdd6DF3 =  rdd6DF2.groupBy("subStringCol").agg(sum("DigitCount").as("DigitCount"))

    var rddDF4 = rdd6DF3.sort(desc("DigitCount"))

    rddDF4 = rddDF4.withColumn("subStringColId",monotonically_increasing_id())

    rddDF4= rddDF4.filter(!col("subStringCol").contains(config.getString("appConfig.replaceSubStringWithChar")))


   var rddDF5= rddDF4.limit(config.getString("appConfig.NumberOfSubstringsTobeConsidered").toInt) //returns the 1st 100 rows.



    rddDF5

  }



}
