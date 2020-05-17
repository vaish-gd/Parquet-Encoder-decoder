import com.roundeights.hasher.Implicits._
class Super1 {






  def getMd5CheckSum(str : String):Int ={
    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"
    var totalSum=0;
    // str.foreach()
    str.foreach( i => {
      totalSum+=(allCharsString.indexOf(i))
    })
    totalSum
  }

  def DigitCount(str:String):String={

    return str.length.toString
  }

  def   getChekSum(str : String) : String  ={

    // val result = "Nidhi".indexOf('h')

    val allCharsString = "!XYZqwertyuiopasdfghjklzxcvbnm1234567890-=!@#$%^&*()_+<>?:|\\[]{}.',"

    var totalSum=0;
    // str.foreach()
    str.foreach( i => {


      //  println(i)

      totalSum+=(allCharsString.indexOf(i))

      //  println(totalSum)




    })
    totalSum+=getMd5CheckSum(str.md5)
   // totalSum
    str.sha256
  }




}
