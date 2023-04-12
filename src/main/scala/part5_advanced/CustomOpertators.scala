package part5_advanced

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{
  Attributes,
  ClosedShape,
  CompletionStrategy,
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
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.Timeout

import scala.collection.mutable
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
  randomGeneratorSource.to(batcherSink).run()
}
