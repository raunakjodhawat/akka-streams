package playground

import scala.annotation.tailrec
import scala.util.Random

object Tambola extends App {
  println("Starting Tambola!")
  val r = new Random()
  @tailrec
  def generateNextNumber(
      l: List[Int] = (for (i <- 1 to 90) yield i).toList,
      count: Int = 1
  ): Unit = {
    if (l.isEmpty) println("Game has ended")
    else {
      val nextNumber = l(r.nextInt(l.size))
      println(s"The [$count] number is: $nextNumber")
      Thread.sleep(1000)
      generateNextNumber(l.filter(_ != nextNumber), count + 1)
    }
  }
  generateNextNumber()
}
