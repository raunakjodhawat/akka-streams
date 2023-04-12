package part5_advanced

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{CompletionStrategy, KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, MergeHub, Sink, Source, BroadcastHub}
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
object Substreams extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")
  import system.dispatcher

  // 1- grouping a stream by a certain function
  val wordsSource = Source(
    List("Akka", "is", "amazing", "learning", "substreams")
  )
  val groups = wordsSource.groupBy(
    30,
    word => if (word.isEmpty) '\u0000' else word.toLowerCase().charAt(0)
  )

//  groups
//    .to(Sink.fold(0)((count, word) => {
//      val newCount = count + 1
//      println(s"I have received: $word, count is $newCount")
//      newCount
//    }))
//    .run()

  // 2- merge substreams back
  val textSource = Source(
    List(
      "I Love Akka Streams",
      "this is amazing",
      "learning from rock the jvm"
    )
  )

  val totalCharacterCountFuture = textSource
    .groupBy(2, string => string.length % 2)
    .map(_.length) // do your expensive computation here
    .mergeSubstreamsWithParallelism(2)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

//  totalCharacterCountFuture.onComplete {
//    case Success(value)     => println(s"total char count: $value")
//    case Failure(exception) => println(s"char count failed: $exception")
//  }

  // 3 - splitting a stream into substreams, when a condition is met
  val text = "I Love Akka Streams\n" +
    "this is amazing\n" +
    "learning from rock the jvm\n"

  val anotherCharCountFuture = Source(text.toList)
    .splitWhen(c => c == '\n')
    .filter(_ != '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

//  anotherCharCountFuture.onComplete {
//    case Success(value)     => println(s"total char count: $value")
//    case Failure(exception) => println(s"char count failed: $exception")
//  }

  // 4 - flattening
  val simpleSource = Source(1 to 5)
//  simpleSource
//    .flatMapConcat(x => Source(x to 3 * x))
//    .runWith(Sink.foreach(println))

  simpleSource
    .flatMapMerge(2, x => Source(x to 3 * x))
    .runWith(Sink.foreach(println))
}
