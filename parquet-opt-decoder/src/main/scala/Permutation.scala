import com.typesafe.config.{Config, ConfigFactory}
import com.roundeights.hasher.Implicits._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}


object Permutation {

  val config: Config = ConfigFactory.load()

  def getMd5CheckSum(str : String):Int ={
    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"
    var totalSum=0;
    // str.foreach()
    var count1= 1
    str.foreach( i => {
      totalSum+=(allCharsString.indexOf(i)*2*count1)
      count1+=1
    })
    totalSum
  }

  def   getChekSum(str : String) : Int  ={

    // val result = "Nidhi".indexOf('h')

    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"

    var totalSum=0;
    // str.foreach()
    var count1= 1
    str.foreach( i => {


      //  println(i)

      totalSum+=(allCharsString.indexOf(i)*2*count1)

      count1+=1

      //  println(totalSum)




    })
    totalSum=getMd5CheckSum(str.sha256)
    totalSum
    //str.sha256
  }


  def permutation(df : DataFrame ) : DataFrame ={

    var df3=df

    def concatFunc(row: Row):Int =  {
      var s1 = row.mkString("")
      getChekSum(s1)
    }
    def combineUdf = udf((row: Row) => concatFunc(row))
    val columns = df.columns
    df.withColumn("contcatenated", combineUdf(struct(columns.map(col): _*)))

    var count =0
    df

  }

  def DigitCountOfX(str:String):Int={
    var m = str.count(_ == 'X')


    m
  }

  def functionTOGetMaximumNumberOfJoinsNeeded(df : DataFrame): Int = {
    val DigitCountUDF = udf((str1: String) => DigitCountOfX(str1))
    var df33 = df.withColumn("countofX", DigitCountUDF(col("building_id")))

    var df12 = df33.agg(max("countofX").as("countofXX"))

    var r1 = df12.rdd.collect()

    var r2=  r1.map(x=> x(0).asInstanceOf[Int])

    r2(0)


  }

  def DecodeFromDataframes(row: Row):String={


    /*  row Df will something be like
 |-- crossJoin0: string (nullable = true)
 |-- crossJoin1: string (nullable = true)
 |-- crossJoin2: string (nullable = true)
 |-- crossJoin3: string (nullable = true)
 |-- building_id: string (nullable = true)
 |--*/


     //gets the 4th column

    //row["building_id"].asInstanceOf[String]

    var s1 = row.getValuesMap(Seq(config.getString("appConfig.columnNameInConsideration")))

    var columnNameInConsiderationValue = s1(config.getString("appConfig.columnNameInConsideration")).toString

    var newString = columnNameInConsiderationValue

    var countOfX =0

    for( i <- newString){


      var compare = config.getString("appConfig.replaceSubStringWithChar").charAt(0)

     // compare = config.getValue("appConfig.replaceSubStringWithChar")

      //compare = 'X'

      if(i == compare){



        var colNo = config.getString("appConfig.crossJoinColumnName")+  countOfX.toString


        var col = row.getValuesMap(Seq(colNo))

        newString= newString.replaceFirst(i.toString, col(colNo).toString)

        countOfX+=1

      }



    }


    newString

  }

  def  Decode(subStringColJoinedDF : DataFrame,buildingIdDF: DataFrame):DataFrame={

  val subStringColJoinedWithbuildingIdDF =  subStringColJoinedDF.crossJoin(buildingIdDF)



    val DecodeFromDataframesUDF = udf((row:Row) => {
      DecodeFromDataframes(row)
    }
    )

    val columns = subStringColJoinedWithbuildingIdDF.columns

    var DecodeDf = subStringColJoinedWithbuildingIdDF.withColumn("DecodedString",DecodeFromDataframesUDF(struct(columns.map(col): _*)))




    val ChekSumUDF = udf((str1:String) => getChekSum(str1))


    var DecodeDf2 = DecodeDf.withColumn("checkSum", ChekSumUDF(col("DecodedString")) )



    DecodeDf2

  }

}

