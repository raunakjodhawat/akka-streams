package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, FanOutShape2, UniformFanInShape}
import akka.stream.scaladsl.{
  Balance,
  Broadcast,
  Flow,
  GraphDSL,
  Merge,
  RunnableGraph,
  Sink,
  Source,
  Zip,
  ZipWith
}

import java.util.Date
object MoreOpenGraphs extends App {
  implicit val system = ActorSystem("more-open-graphs")
  /*
  Example: Max3 operator
  - 3 inputs of type int
  - the maximum of the three
   */

  val max3StaticGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val max1 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))
    val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))

    max1.out ~> max2.in0

    UniformFanInShape(max2.out, max1.in0, max1.in1, max2.in1)
  }

  val source1 = Source(1 to 10)
  val source2 = Source((1 to 10).map(_ => 5))
  val source3 = Source((1 to 10).reverse)

  val maxSink = Sink.foreach[Int](x => println(s"Max is: $x"))

  val max3RunnableGraph =
    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val max3Shape = builder.add(max3StaticGraph)
      source1 ~> max3Shape.in(0)
      source2 ~> max3Shape.in(1)
      source3 ~> max3Shape.in(2)

      max3Shape.out ~> maxSink
      ClosedShape
    })
//  max3RunnableGraph.run()

  // same for UniformFanOutShape
  /*
  Non Uniform fan out shape
  Processing bank transactions
  Txn Suspicious if amount > 10000

  Streams component for txns
  - output 1: let transaction go through (unmodified)
  - output 2: only give back suspicious txn id's
   */

  case class Transaction(
      id: String,
      source: String,
      recipient: String,
      amount: Int,
      date: Date
  )
  val transactionSource = Source(
    List(
      Transaction("123123", "raunak", "raunak", 100, new Date),
      Transaction("98039182", "abc", "abc", 1000000, new Date),
      Transaction("90809123", "def", "def", 7000, new Date)
    )
  )

  val bankProcessor = Sink.foreach[Transaction](println)
  val suspiciousProcessor =
    Sink.foreach[String](txId => println(s"Suspicious transaction ID: $txId"))

  val suspiciousTxnStaticGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val broadcast = builder.add(Broadcast[Transaction](2))
    val suspiciousTransactionFilter =
      builder.add(Flow[Transaction].filter(txn => txn.amount > 10000))
    val txnIdExtractor =
      builder.add(Flow[Transaction].map[String](txn => txn.id))

    broadcast.out(0) ~> suspiciousTransactionFilter ~> txnIdExtractor

    new FanOutShape2(broadcast.in, broadcast.out(1), txnIdExtractor.out)
  }

  val suspiciousTxnRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val suspiciousTxnShape = builder.add(suspiciousTxnStaticGraph)

      transactionSource ~> suspiciousTxnShape.in
      suspiciousTxnShape.out0 ~> bankProcessor
      suspiciousTxnShape.out1 ~> suspiciousProcessor

      ClosedShape
    }
  )
  suspiciousTxnRunnableGraph.run()
}
