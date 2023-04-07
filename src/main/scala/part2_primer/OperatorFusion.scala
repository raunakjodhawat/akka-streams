package part2_primer

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.{Flow, Sink, Source}

object OperatorFusion extends App {
  implicit val system = ActorSystem("operator-fusion")

  val simpleSource = Source(1 to 10000)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleFlow2 = Flow[Int].map(_ * 1000)
  val simpleSink = Sink.foreach[Int](println)

  // this runs on the same actor
  // simpleSource.via(simpleFlow).via(simpleFlow2).to(simpleSink).run()
  // operator/component fusion

  "equivalent behaviour"
  class SimpleActor extends Actor {
    override def receive: Receive = {
      case x: Int => {
        val x1 = x + 1
        val x2 = x1 * 1000
        println(x2)
      }
    }
  }
  val simpleActor = system.actorOf(Props[SimpleActor], "simple-actor")
//  (1 to 1000).foreach(simpleActor ! _)

  // if operations are time expensive operation fusion is not good
  // complex flows
  val complexFlow = Flow[Int].map { x =>
    Thread.sleep(1000)
    x + 1
  }

  val complexFlow2 = Flow[Int].map { x =>
    Thread.sleep(1000)
    x * 10
  }
  // simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink).run()

  // when operations are expensive, we should break them into separate actors
  // async boundary
  simpleSource
    .via(complexFlow)
    .async
    .via(complexFlow2)
    .async
    .to(simpleSink)
    .run()

  // ordering gurantees
  Source(1 to 3)
    .map(element => { println(s"Flow A: $element"); element })
    .async
    .map(element => { println(s"Flow B: $element"); element })
    .async
    .map(element => { println(s"Flow C: $element"); element })
    .async
    .runWith(Sink.ignore)
}
