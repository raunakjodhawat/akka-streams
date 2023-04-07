package part2_primer

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

object BackpressureBasics extends App {
  implicit val system = ActorSystem("BackpressureBasics")

  val fastSource = Source(1 to 1000)
  val slowSink = Sink.foreach[Int] { x =>
    Thread.sleep(1000)
    println(s"Sink: $x")
  }
  // fastSource.to(slowSink).run() // not backpressure

  // fastSource.async.to(slowSink).run() // actual backpressure

  val simpleFlow = Flow[Int].map { x =>
    println(s"incoming: $x")
    x + 1
  }
//  fastSource.async.via(simpleFlow).async.to(slowSink).run()
  // buffering is default
  /*
  reactions to backpressure (in order):
    - try to slow down if possible
    - buffer elements until there's more demand
    - drop down elements from the buffer if it overflows
    - tear down/kill the whole stream (failure)
   */
  val bufferedFlow =
    simpleFlow.buffer(10, overflowStrategy = OverflowStrategy.dropBuffer)
  fastSource.async.via(bufferedFlow).async.to(slowSink).run()

  /*
  1-16: nobody is backpressured
  17-26: flow will buffer, flow starts dropping at the next element
  26-1000: flow will always drop the oldest element
   */

  // throttle
  import scala.concurrent.duration._
  fastSource.throttle(2, 1.seconds).runWith(Sink.foreach(println))
}
