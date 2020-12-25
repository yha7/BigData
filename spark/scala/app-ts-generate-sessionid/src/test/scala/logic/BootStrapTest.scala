package logic
import org.scalatest._
import matchers._
import org.scalatest.flatspec.AnyFlatSpec
class BootStrapTest extends AnyFlatSpec{
  "A method generateSessionId" should "return seq of tuples" in {
    val user_id = "u1"

    val clickList: Seq[String] = Seq("2018-01-01 11:00:00",
      "2018-01-01 12:10:00",
      "2018-01-01 13:00:00",
      "2018-01-01 13:25:00",
      "2018-01-01 14:40:00",
      "2018-01-01 15:10:00",
      "2018-01-01 16:20:00",
      "2018-01-01 16:50:00")

    val tsList: Seq[Long] = Seq(0, 4200, 3000, 1500, 4500, 1800, 4200, 1800)

   /* val expectedSequence = logic.BootStrap.generateSessionId(user_id,clickList,tsList)
    expectedSequence.foreach(println(_))*/

  }

}
