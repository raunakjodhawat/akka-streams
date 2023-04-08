package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{
  Balance,
  Broadcast,
  Flow,
  GraphDSL,
  Merge,
  RunnableGraph,
  Sink,
  Source,
  Zip
}

import scala.concurrent.duration.DurationInt

object GraphBasics extends App {
  implicit val system = ActorSystem("Graph-Basic")

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(_ + 1)
  val multiplier = Flow[Int].map(_ * 10)
  val output = Sink.foreach[(Int, Int)](println)

  // step 1: Setting up the fundamentals for the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] => // builder is a mutable data structure, which gets mutated in the code block below
        import GraphDSL.Implicits._

        // step 2: Add necessary components of this graph
        val broadcast = builder.add(Broadcast[Int](2)) // fan-out operators
        val zip = builder.add(Zip[Int, Int]) // fan-in operator

        // step 3: Tying up the components
        input ~> broadcast
        broadcast.out(0) ~> incrementer ~> zip.in0
        broadcast.out(1) ~> multiplier ~> zip.in1
        zip.out ~> output

        // step 4: Return a closed shape
        ClosedShape // Freeze the builder's stage
        // must return a shape
    } // must be a graph (static graph)
  ) // runnable graph
//  graph.run()

  // Exercises
  /*
  feed 1 source and feed into two sinks
   */
  val output2 =
    Sink.foreach[(Int, Int)](x => println(s"hello: ${x._1} :: ${x._2}"))
  val oneSourceToTwoSink = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[Int](2))
        val interZip = builder.add(Zip[Int, Int])

        input ~> broadcast
        broadcast ~> incrementer ~> interZip.in0
        broadcast ~> multiplier ~> interZip.in1

        val newBroadcast = builder.add(Broadcast[(Int, Int)](2))
        interZip.out ~> newBroadcast

        newBroadcast ~> output
        newBroadcast ~> output2

        ClosedShape
      }
    }
  )
//
//  oneSourceToTwoSink.run()

  val slowSource = input.throttle(5, 1.second)
  val fastSource = input.throttle(2, 1.second)
  val sink1 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1: [Count]: $count")
    count + 1
  })
  val sink2 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 2: [Count]: $count")
    count + 1
  })
  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](2))
      fastSource ~> merge ~> balance ~> sink1
      slowSource ~> merge

      balance ~> sink2
      ClosedShape
    }
  )
  balanceGraph.run()
}
