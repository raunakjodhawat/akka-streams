package part2_primer

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
object MaterialzingStreams extends App {
  implicit val system = ActorSystem("MaterializingStreams")

  val simpleGraph = Source(1 to 10).to(Sink.foreach(println))
//  val simpleMaterializedValue = simpleGraph.run()

  val source = Source(1 to 10)
  val sink = Sink.reduce[Int]((a, b) => a + b)
  val sumFuture = source.runWith(sink)
  sumFuture.onComplete {
    case Success(value) => println(s"the sum of all elements is $value")
    case Failure(ex) =>
      println(s"the sum of the elements could not be computed: $ex")
  }

  // choosing materialized values
  val simpleSource = Source(1 to 10)
  val simpleFlow = Flow[Int].map(x => x + 1)
  val simpleSink = Sink.foreach[Int](println)
  val graph =
    simpleSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right)
  graph.run().onComplete {
    case Success(_) => println(s"Stream processing complete")
    case Failure(ex) =>
      println(s"Stream processing failed with: $ex")
  }
  // sugars
  val sum = Source(1 to 10).runWith(
    Sink.reduce[Int](_ + _)
  ) // source.to(Sink.reduce)(keep.right)

  Source(1 to 10).runReduce[Int](_ + _) // same

  // backwards
  Sink
    .foreach[Int](println)
    .runWith(Source.single(42)) // source(..).to(sink...).run()

  // both ways
  Flow[Int].map(x => 2 * x).runWith(simpleSource, simpleSink)

  println("exercise")
  // exercise
  val names = List("alice", "charly", "bob", "daniel")
  val exSource = Source(names)
  val exFlow = Flow[String]
  val exSink = Sink.last[String]
  exSource.toMat(exSink)(Keep.right).run()
  exSource.runWith(Sink.last[String])

  val sentences =
    List("this is a sentence 1", "this is a sentence 2", "this is a sentence 3")
  val ex2Source = Source(sentences)
  val ex2Flow = Flow[String].map(_.split(" ").length)
  val ex2Sink = Sink.reduce[Int](_ + _)
  val g1 =
    ex2Source.viaMat(ex2Flow)(Keep.right).toMat(ex2Sink)(Keep.right).run()
  val g2 = ex2Source.via(exFlow).to(exSink).run()
  val g3 = ex2Source.runFold(0)((a, b) => a + b.split(" ").length)
  g1.onComplete { case Success(value) =>
    println(value)
  }

  g3.onComplete { case Success(value) =>
    println(value)
  }

}
