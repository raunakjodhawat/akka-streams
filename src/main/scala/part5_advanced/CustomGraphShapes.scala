package part5_advanced

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{
  ClosedShape,
  CompletionStrategy,
  Inlet,
  KillSwitches,
  Outlet,
  OverflowStrategy,
  Shape
}
import akka.stream.scaladsl.{
  Balance,
  BroadcastHub,
  Flow,
  GraphDSL,
  Keep,
  Merge,
  MergeHub,
  RunnableGraph,
  Sink,
  Source
}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
object CustomGraphShapes extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")

  import system.dispatcher

  // balance 2x3 shape
  case class Balance2x3(
      in0: Inlet[Int],
      in1: Inlet[Int],
      out0: Outlet[Int],
      out1: Outlet[Int],
      out2: Outlet[Int]
  ) extends Shape {
    override val inlets: Seq[Inlet[_]] = Seq(in0, in1)
    override val outlets: Seq[Outlet[_]] = Seq(out0, out1, out2)
    override def deepCopy(): Shape = Balance2x3(
      in0.carbonCopy(),
      in1.carbonCopy(),
      out0.carbonCopy(),
      out1.carbonCopy(),
      out2.carbonCopy()
    )
  }

  val balance2x3Impl = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val merge = builder.add(Merge[Int](2))
    val balance = builder.add(Balance[Int](3))

    merge ~> balance
    Balance2x3(
      merge.in(0),
      merge.in(1),
      balance.out(0),
      balance.out(1),
      balance.out(2)
    )
  }

  val balance2x3Graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val slowSource = Source(Stream.from(1)).throttle(1, 1.second)
      val fastSource = Source(Stream.from(1)).throttle(2, 1.second)

      def createSink(index: Int) = Sink.fold(0)((count: Int, element: Int) => {
        println(
          s"Received at :$index element: $element, current count is: $count"
        )
        count + 1
      })

      val sink1 = builder.add(createSink(1))
      val sink2 = builder.add(createSink(2))
      val sink3 = builder.add(createSink(3))

      val balance2x3 = builder.add(balance2x3Impl)

      slowSource ~> balance2x3.in0
      fastSource ~> balance2x3.in1
      balance2x3.out0 ~> sink1
      balance2x3.out1 ~> sink2
      balance2x3.out2 ~> sink3

      ClosedShape
    }
  )
//  balance2x3Graph.run()

  /*
  Exercise: generalize the balance component make it M * N and expand it to generic
   */

  case class BalanceComponent[A](
      inlets: Seq[Inlet[A]],
      outlets: Seq[Outlet[A]]
  ) extends Shape {
    override def deepCopy(): Shape = BalanceComponent[A](
      inlets.map(_.carbonCopy()),
      outlets.map(_.carbonCopy())
    )
  }

  def balanceComponentImpl[A](m: Int, n: Int) = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      val merge = builder.add(Merge[A](m))
      val balance = builder.add(Balance[A](n))

      merge ~> balance
      val inlets: Seq[Inlet[A]] = for (i <- 0 until m) yield merge.in(i)
      val outlets: Seq[Outlet[A]] = for (i <- 0 until n) yield balance.out(i)
      BalanceComponent(inlets, outlets)
  }

  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val slowSource = Source(Stream.from(1)).throttle(1, 1.second)
      val fastSource = Source(Stream.from(1)).throttle(2, 1.second)

      def createSink(index: Int) = Sink.fold(0)((count: Int, element: Int) => {
        println(
          s"Received at :$index element: $element, current count is: $count"
        )
        count + 1
      })

      val sink1 = builder.add(createSink(1))
      val sink2 = builder.add(createSink(2))
      val sink3 = builder.add(createSink(3))

      val balance = builder.add(balanceComponentImpl[Int](2, 3))

      slowSource ~> balance.inlets(0)
      fastSource ~> balance.inlets(1)
      balance.outlets(0) ~> sink1
      balance.outlets(1) ~> sink2
      balance.outlets(2) ~> sink3

      ClosedShape
    }
  )
  balanceGraph.run()
}
