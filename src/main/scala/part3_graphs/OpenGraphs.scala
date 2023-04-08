package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, FlowShape, SinkShape, SourceShape}
import akka.stream.scaladsl.{
  Balance,
  Broadcast,
  Concat,
  Flow,
  GraphDSL,
  Merge,
  RunnableGraph,
  Sink,
  Source,
  Zip
}

import scala.concurrent.duration.DurationInt
object OpenGraphs extends App {
  implicit val system = ActorSystem("open-graph")
  /*
  A composite source that concatenates 2 source
  - emits all elements from the first source
  - then the second one
   */
  val firstSource = Source(1 to 10)
  val secondSource = Source(42 to 1000)

  val sourceGraph = Source.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val concat = builder.add(Concat[Int](2))
      firstSource ~> concat
      secondSource ~> concat
      SourceShape(concat.out)
    }
  )
//  sourceGraph.to(Sink.foreach(println)).run()

  /*
  Complex sink
   */
  val sink1 = Sink.foreach[Int](x => println(s"thing 1: $x"))
  val sink2 = Sink.foreach[Int](x => println(s"thing 2: $x"))

  val sinkGraph = Sink.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[Int](2))
      broadcast ~> sink1
      broadcast ~> sink2
      SinkShape(broadcast.in)
    }
  )
//  firstSource.to(sinkGraph).run()

  val incrementerFlow = Flow[Int].map(_ + 1)
  val multiplierFlow = Flow[Int].map(_ * 10)

  val flowGraph = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val incrementerShape = builder.add(incrementerFlow)
      val multiplierShape = builder.add(multiplierFlow)

      incrementerShape ~> multiplierShape
      FlowShape(incrementerShape.in, multiplierShape.out)
    } // static graph
  ) // component

  firstSource.via(flowGraph).to(Sink.foreach(println)).run()

  def fromSinkAndSource[A, B](
      sink: Sink[A, _],
      source: Source[B, _]
  ): Flow[A, B, _] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      val sourceShape = builder.add(source)
      val sinkShape = builder.add(sink)
      FlowShape(sinkShape.in, sourceShape.out)
    }
  )

  val f = Flow.fromSinkAndSourceCoupled(
    Sink.foreach[String](println),
    Source(1 to 10)
  )

}
