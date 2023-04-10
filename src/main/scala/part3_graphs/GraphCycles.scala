package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, Outlet, OverflowStrategy, UniformFanInShape}
import akka.stream.scaladsl.{
  Balance,
  Broadcast,
  Concat,
  Flow,
  GraphDSL,
  Merge,
  MergePreferred,
  RunnableGraph,
  Sink,
  Source,
  Zip,
  ZipWith
}

object GraphCycles extends App {
  implicit val system = ActorSystem("Graph-cycles")

  val accelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    val incrementerShape = builder.add(Flow[Int].map { x =>
      println(s"Accelerating: $x")
      x + 1
    })
    sourceShape ~> mergeShape ~> incrementerShape
    mergeShape <~ incrementerShape
    ClosedShape
  }

//  RunnableGraph.fromGraph(accelerator).run()
  // graph cycle deadlock!

  /*
  Solution 1: MergePreferred
   */

  val actualAccelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(MergePreferred[Int](1))
    val incrementerShape = builder.add(Flow[Int].map { x =>
      println(s"Accelerating: $x")
      x + 1
    })
    sourceShape ~> mergeShape ~> incrementerShape
    mergeShape.preferred <~ incrementerShape
    ClosedShape
  }
//  RunnableGraph.fromGraph(actualAccelerator).run()

  /*
  Solution 2: buffers
   */

  val bufferAccelerator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val sourceShape = builder.add(Source(1 to 100))
    val mergeShape = builder.add(Merge[Int](2))
    val repeaterShape =
      builder.add(Flow[Int].buffer(10, OverflowStrategy.dropHead).map { x =>
        println(s"Accelerating: $x")
        Thread.sleep(100)
        x
      })
    sourceShape ~> mergeShape ~> repeaterShape
    mergeShape <~ repeaterShape
    ClosedShape
  }
//  RunnableGraph.fromGraph(bufferAccelerator).run()
  /*
  Cycles risk deadlocking
   - add bounds to the number of elements in the cycle
   boundedness vs liveness
   */

  /*
  Challenge: create a fan-in shape
  - two inputs which will be fed with exactly one number
  - output will emit an infinite fibonacci sequence based off those 2 numbers
   1, 1, 2, 3, 5, 8
   Hint: use ZipWith and cycles, MergePreferred
   */

  val finbonaaciStream = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val sourceShape1 = builder.add(Source.single(1))
    val sourceShape2 = builder.add(Source.single(2))

    val zipShape = builder.add(Zip[Int, Int])
    val mergeShape = builder.add(MergePreferred[(Int, Int)](1))
    val flowShape = builder.add(Flow[(Int, Int)].map(x => (x._1 + x._2, x._1)))
    val broadcastShape = builder.add(Broadcast[(Int, Int)](2))
    val extractLastShape = builder.add(Flow[(Int, Int)].map(x => x._1))

    zipShape.out ~> mergeShape ~> flowShape ~> broadcastShape ~> extractLastShape
    mergeShape.preferred <~ broadcastShape

    val fanInShape =
      UniformFanInShape(extractLastShape.out, zipShape.in0, zipShape.in1)
    sourceShape1 ~> fanInShape.in(0)
    sourceShape2 ~> fanInShape.in(1)
    fanInShape.out ~> Sink.foreach[Int](println)
    ClosedShape
  }

  RunnableGraph.fromGraph(finbonaaciStream).run()
}
