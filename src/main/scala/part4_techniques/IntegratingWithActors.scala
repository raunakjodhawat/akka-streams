package part4_techniques

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt
object IntegratingWithActors extends App {
  implicit val system = ActorSystem("IntegratingWithActors")

  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s: String =>
        log.info(s"Just received a string: $s")
        sender() ! s"$s$s"
      case n: Int =>
        log.info(s"just received a number: $n")
        sender() ! (2 * n)
      case _ =>
    }
  }
  val simpleActor = system.actorOf(Props[SimpleActor], "simpleActor")

  val numberSource = Source(1 to 10)

  // actor as a flow
  implicit val timeout = Timeout(2.seconds)
  val actorBasedFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

//  numberSource.via(actorBasedFlow).to(Sink.ignore).run()
//  numberSource.ask[Int](parallelism = 4)(simpleActor).to(Sink.ignore).run()

  /*
  Actor as a source
   */
  val actorPoweredSource = Source.actorRef(
    completionMatcher = { case Done =>
      CompletionStrategy.immediately
    },
    failureMatcher = PartialFunction.empty,
    bufferSize = 10,
    overflowStrategy = OverflowStrategy.dropHead
  )
  val materializedActorRef = actorPoweredSource
    .to(
      Sink.foreach[Int](n => println(s"Actor powered flow got: $n"))
    )
    .run()
  materializedActorRef ! 10
  materializedActorRef ! Done

  // Actor as a destination/sink
  /*
  - an init message
  - an ack message to confirm the reception
  - a complete message
  - a function to generate a message in case the stream throws an exception
   */

  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFail(ex: Throwable)

  class DestinationActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case StreamInit =>
        log.info("Stream initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.warning(s"Stream Failed: $ex")
      case message =>
        log.info(s"Message: $message")
        sender() ! StreamAck

    }
  }

  val destinationActor =
    system.actorOf(Props[DestinationActor], "destination-actor")

  val actorPoweredSink = Sink.actorRefWithBackpressure[Int](
    destinationActor,
    onInitMessage = StreamInit,
    onCompleteMessage = StreamComplete,
    ackMessage = StreamAck,
    onFailureMessage = throwable => StreamFail(throwable)
  )

  Source(1 to 10).to(actorPoweredSink).run()
}
