package part5_advanced

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{
  Attributes,
  ClosedShape,
  CompletionStrategy,
  FlowShape,
  Inlet,
  KillSwitches,
  Outlet,
  OverflowStrategy,
  Shape,
  SinkShape,
  SourceShape
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
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler
}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Random

object CustomOpertators extends App {
  implicit val system = ActorSystem("CustomOperators")

  // 1 - a custom source which emits random numbers until canceled
  class RandomNumberGenerator(max: Int)
      extends GraphStage[ /*Step 0: define the shape*/ SourceShape[Int]] {
    // step 1: define the ports and the component-specific members
    val outPort = Outlet[Int]("randomGenerator")
    val random = new Random()

    // step 3: create the logic
    override def shape: SourceShape[Int] = SourceShape(outPort)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        // step 4:
        // define mutable state
        // implement my logic here
        setHandler(
          outPort,
          new OutHandler {
            // when there is demand from downstream
            override def onPull(): Unit = {
              // emit a new element
              val nextNumber = random.nextInt(max)
              // push it out of the outport
              push(outPort, nextNumber)
            }
          }
        )
      }

  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))
//  randomGeneratorSource.runWith(Sink.foreach(println))

  // 2 - a custom sink that prints elements in batches of a given size

  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {
    val inPort = Inlet[Int]("batcher")
    override def shape: SinkShape[Int] = SinkShape[Int](inPort)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        override def preStart(): Unit = {
          pull(inPort)
        }
        // mutable state
        val batch = new mutable.Queue[Int]
        setHandler(
          inPort,
          new InHandler {
            // when the upstream wants to send me an element
            override def onPush(): Unit = {
              val nextElement = grab(inPort)
              batch.enqueue(nextElement)
              Thread.sleep(1000)
              if (batch.size >= batchSize) {
                println(
                  s"New batch: " + batch
                    .dequeueAll(_ => true)
                    .mkString("[", ", ", "]")
                )
              }
              pull(inPort) // send demand upstream
            }

            override def onUpstreamFinish(): Unit = {
              if (batch.nonEmpty) {
                println(
                  s"New batch: " + batch
                    .dequeueAll(_ => true)
                    .mkString("[", ", ", "]")
                )
                println("Stream finished")
              }
            }
          }
        )
      }
  }

  val batcherSink = Sink.fromGraph(new Batcher(10))
//  randomGeneratorSource.to(batcherSink).run()

  /*
  Exercise: a custom flow - a simple filter flow
  - 2 ports: an input port and an output port

   */

  class CustomFlow[A](
      predicate: A => Boolean
  ) extends GraphStage[FlowShape[A, A]] {
    val inputPort = Inlet[A]("filterIn")
    val outputPort = Outlet[A]("filterOut")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        setHandler(
          outputPort,
          new OutHandler {
            override def onPull(): Unit = pull(inputPort)
          }
        )

        setHandler(
          inputPort,
          new InHandler {
            override def onPush(): Unit = {
              try {
                val nextElem: A = grab[A](inputPort)
                if (predicate(nextElem)) {
                  push(outputPort, nextElem)
                } else {
                  pull(inputPort)
                }
              } catch {
                case e: Throwable => failStage(e)
              }
            }
          }
        )
      }

    override def shape: FlowShape[A, A] = FlowShape[A, A](inputPort, outputPort)
  }

//  randomGeneratorSource.to(batcherSink).run()
  val customFlow = Flow.fromGraph(
    new CustomFlow[Int]((x: Int) => x % 2 == 0)
  )
//  randomGeneratorSource.via(customFlow).to(batcherSink).run()

  /*
  Materialized values in graph stages
   */
  // 3- a flow that counts the number of elements that go through it
  class CounterFlow[A]
      extends GraphStageWithMaterializedValue[FlowShape[A, A], Future[Int]] {
    val inputPort = Inlet[A]("filterIn")
    val outputPort = Outlet[A]("filterOut")
    override val shape = FlowShape(inputPort, outputPort)

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes
    ): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // setting mutable state
        var counter = 0
        setHandler(
          outputPort,
          new OutHandler {
            override def onPull(): Unit = pull(inputPort)

            override def onDownstreamFinish(cause: Throwable): Unit = {
              promise.success(counter)
              super.onDownstreamFinish(cause)
            }
          }
        )
        setHandler(
          inputPort,
          new InHandler {
            override def onPush(): Unit = {
              val nextElement = grab(inputPort)
              counter += 1
              // pass it on
              push(outputPort, nextElement)
            }

            override def onUpstreamFinish(): Unit = {
              promise.success(counter)
              super.onUpstreamFinish()
            }

            override def onUpstreamFailure(ex: Throwable): Unit = {
              promise.failure(ex)
              super.onUpstreamFailure(ex)
            }
          }
        )
      }
      (logic, promise.future)
    }
  }

  import system.dispatcher
  import scala.util.{Failure, Success}
  val counterFlow = Flow.fromGraph(new CounterFlow[Int])
  val countFuture = Source(1 to 10)
    .viaMat(counterFlow)(Keep.right)
    .to(
      Sink.foreach[Int](x =>
        if (x == 7) throw new RuntimeException("gotcha")
        else println(x)
      )
    )
    .run()
  countFuture.onComplete {
    case Success(value) => println(s"the number of elements passed: $value")
    case Failure(ex)    => println(s"Counting the element failed: $ex")
  }
}
