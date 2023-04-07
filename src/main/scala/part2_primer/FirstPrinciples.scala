package part2_primer

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FirstPrinciples extends App {
  implicit val system = ActorSystem("FirstPrinciples")

  // sources
  val source = Source(1 to 10)
  // sink
  val sink = Sink.foreach[Int](println)

  val graph = source.to(sink) // connects source to sink
//  graph.run()

  // flows transform elements
  val flow = Flow[Int].map(x => x + 1)
  val sourceWithFlow = source.via(flow)
  val flowWithSink = flow.to(sink)

//  sourceWithFlow.to(sink).run()
//  source.to(flowWithSink).run()
//  source.via(flow).to(sink).run()

  // nulls are not allowed as per reactive streams specification
//  val illegalSource = Source.single[String](null)
//  illegalSource.to(Sink.foreach(println)).run()
  // use options

  // various kinds of sources
  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1, 2, 3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(
    LazyList.from(1)
  ) // do not confuse with an akka stream
  val futureSource = Source.future(Future(42))

  // sinks
  val theMostBoringSink = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink =
    Sink.head[Int] // just retrieves the head and then closes the stream
  val foldSink = Sink.fold[Int, Int](0)((a, b) => a + b)

  // flows - usvally mapped to collection operators
  val mapFlow = Flow[Int].map(x => 2 * x)
  val takeFlow = Flow[Int].take(5)
  // drop, filter
  // NOT have flatMap

  // source -> flow -> flow -> ..... -> sink
  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)
//  doubleFlowGraph.run()

  val mapSource = Source(1 to 10).map(x =>
    x * 10
  ) // Source(1 to 10).via(Flow[Int].map(x => x * 2))
  // run streams directly
//  mapSource.runForeach(
//    println
//  ) // mapSource.to(Sink.foreach[Int](println)).run()

  // OPERATORS = components

  // Exercise
  val names = List("Alice", "Bob", "Charly", "David", "Martin", "AkkaStreams")
  val nameSource = Source(names)
  val longNameFlow = Flow[String].filter(x => x.length > 5)
  val limitFlow = Flow[String].take(2)
  val nameSink = Sink.foreach[String](println)

  nameSource.via(longNameFlow).via(limitFlow).to(nameSink).run()
}
