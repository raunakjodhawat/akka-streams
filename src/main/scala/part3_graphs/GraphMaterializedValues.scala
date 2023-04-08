package part3_graphs

import akka.actor.ActorSystem
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, FlowShape, SinkShape}
import akka.stream.scaladsl.{
  Balance,
  Broadcast,
  Flow,
  GraphDSL,
  Keep,
  Merge,
  RunnableGraph,
  Sink,
  Source,
  Zip
}

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
object GraphMaterializedValues extends App {
  implicit val system = ActorSystem("GraphMaterializedValues")

  val wordSource = Source(List("Ak", "is", "awrsome", "rock", "the", "kvm"))
  val printer = Sink.foreach[String](println)
  val counter = Sink.fold[Int, String](0)((count, _) => count + 1)

  /*
  A composite component (Sink)
  - prints out all strings which are lowercase
  - Counts the strings that are short (<5 chars)
   */

  val complexWordSink = Sink.fromGraph(
    GraphDSL.createGraph(printer, counter)((printerMatValue, counterMatValue) =>
      counterMatValue
    ) { implicit builder => (printerShape, counterShape) =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[String](2))
      val lowerCaseFilter =
        builder.add(Flow[String].filter(word => word == word.toLowerCase))
      val shortStringFilter = builder.add(Flow[String].filter(_.length < 5))

      broadcast ~> lowerCaseFilter ~> printerShape
      broadcast ~> shortStringFilter ~> counterShape
      SinkShape(broadcast.in)
    }
  )

  val shortStringCountFuture =
    wordSource.toMat(complexWordSink)(Keep.right).run()
  shortStringCountFuture.onComplete {
    case Success(value) =>
      println(s"total count: $value")
    case Failure(e) => println(e)
  }

  /*
  Exercise
   */
  def enhancedFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {
    val counterSink = Sink.fold[Int, B](0)((count, _) => count + 1)
    Flow.fromGraph(
      GraphDSL.createGraph(counterSink) {
        implicit builder => counterSinkShape =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[B](2))
          val orignalFlowShape = builder.add(flow)
          orignalFlowShape ~> broadcast ~> counterSinkShape
          FlowShape(orignalFlowShape.in, broadcast.out(1))
      }
    )
  }
  val simpleSource = Source(1 to 42)
  val simpleFlow = Flow[Int].map(x => x)
  val simpleSink = Sink.ignore

  val enhancedFlowCountFuture = simpleSource
    .viaMat(enhancedFlow(simpleFlow))(Keep.right)
    .toMat(simpleSink)(Keep.left)
    .run()

  enhancedFlowCountFuture.onComplete {
    case Success(value) =>
      println(s"total count 2: $value")
    case Failure(e) => println(e)
  }
}
