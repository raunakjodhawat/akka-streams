package part5_advanced
import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{CompletionStrategy, KillSwitches, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, MergeHub, Sink, Source, BroadcastHub}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt

object DynamicStreamHandling extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")
  import system.dispatcher
  // #1: Kill switch
//  val killSwitchFlow = KillSwitches.single[Int]
  val counter = Source(Stream.from(1)).throttle(1, 1.second).log("counter")
//  val sink = Sink.ignore
//
//  val killSwitch =
//    counter.viaMat(killSwitchFlow)(Keep.right).toMat(sink)(Keep.left).run()
//
//  import system.dispatcher
//  system.scheduler.scheduleOnce(3.seconds) {
//    killSwitch.shutdown()
//  }

  val anotherCounter =
    Source(Stream.from(1)).throttle(2, 1.second).log("anotherCounter")
  val sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll")

  counter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
  anotherCounter.via(sharedKillSwitch.flow).runWith(Sink.ignore)

  system.scheduler.scheduleOnce(3.seconds) {
    sharedKillSwitch.shutdown()
  }

  // MergeHub
  val dynamicMerge = MergeHub.source[Int]
  val materializedSink = dynamicMerge.to(Sink.foreach[Int](println)).run()

  // use this sink any time we like
//  Source(1 to 10).runWith(materializedSink)
//  counter.runWith(materializedSink)

  // BroadcastHub
  val dynamicBroadcast = BroadcastHub.sink[Int]

  val materializedSource = Source(1 to 100).runWith(dynamicBroadcast)

  materializedSource.runWith(Sink.ignore)
  materializedSource.runWith(Sink.foreach[Int](println))

  /*
  Challenge - combine a mergeHub and a broadcastHub
   */
  // pub-sub component
  materializedSource.runWith(dynamicBroadcast)

  val merge = MergeHub.source[String]
  val bcast = BroadcastHub.sink[String]

  val (publisherPort, subscriberPort) = merge.toMat(bcast)(Keep.both).run()

  subscriberPort.runWith(Sink.foreach(e => println(s"I received: $e")))
  subscriberPort
    .map(string => string.length)
    .runWith(Sink.foreach(n => println(s"I have received a number: $n")))

  Source(List("1", "2", "3")).runWith(publisherPort)
  Source(List("234", "890", "999")).runWith(publisherPort)
  Source.single("Streaaams").runWith(publisherPort)
}
