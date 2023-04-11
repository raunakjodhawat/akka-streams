package part4_techniques
import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.util.Timeout

import java.util.Date
import scala.concurrent.duration.DurationInt
import scala.util.Random
object FaultTolerance extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")

  // 1 - logging
  val faultySource =
    Source(1 to 10).map(e => if (e == 6) throw new RuntimeException else e)
//  faultySource.log("trackingElements").to(Sink.ignore).run()

  // 2 - gracefully terminating the stream
//  faultySource
//    .recover { case _: RuntimeException =>
//      Int.MinValue
//    }
//    .log("gracefulSource")
//    .to(Sink.ignore)
//    .run()

  // 3 - recover with another streams
//  faultySource
//    .recoverWithRetries(
//      3,
//      { case _: RuntimeException =>
//        Source(90 to 99)
//      }
//    )
//    .log("recoverWithRetires")
//    .to(Sink.ignore)
//    .run()

  // 4 - backoff supervision
  val restartSource = RestartSource
    .onFailuresWithBackoff(
      minBackoff = 1.second,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )(() => {
      val randomNumber = new Random().nextInt(20)
      Source(1 to 10).map(elem =>
        if (elem == randomNumber) throw new RuntimeException else elem
      )
    })
//  restartSource
//    .log("restartBackoff")
//    .to(Sink.ignore)
//    .run()

  // 5 - supervision strategy
  val numbers = Source(1 to 20)
    .map(n => if (n == 13) throw new RuntimeException("Bad luck") else n)
    .log("supervision")
  val superviseidNumbers =
    numbers.withAttributes(ActorAttributes.supervisionStrategy {
      /*
      Resume => skip the faulty elements
      Stop => stop the stream
      Restart => resume + clear internal state
       */
      case _: RuntimeException => Resume
      case _                   => Stop
    })
  superviseidNumbers.to(Sink.ignore).run()
}
