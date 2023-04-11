package part4_techniques

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import java.util.Date
import scala.concurrent.duration.DurationInt
object AdvancedBackPressure extends App {
  implicit val system = ActorSystem("AdvancedBackpressure")

  // control backpressure
  val controlledFlow =
    Flow[Int].map(_ * 2).buffer(10, OverflowStrategy.dropHead)

  case class PagerEvent(description: String, date: Date, nInstances: Int = 1)
  case class Notification(email: String, pagerEvent: PagerEvent)

  val events = List(
    PagerEvent("pg1", new Date),
    PagerEvent("pg2", new Date),
    PagerEvent("pg3", new Date),
    PagerEvent("pg4", new Date)
  )

  val eventSource = Source(events)
  val onCallEngineer = "raunak@gmail.com" // a fast service for fetching emails

  def sendEmail(notification: Notification) = println(
    s"Dear ${notification.email}, you have an event ${notification.pagerEvent}"
  )

  val notificationSink = Flow[PagerEvent]
    .map(event => Notification(onCallEngineer, event))
    .to(Sink.foreach[Notification](sendEmail))

  // standard
//  eventSource.to(notificationSink).run()

  /*
  un-backpressurable source
   */
  def sendEmailSlow(notification: Notification) = {
    Thread.sleep(1000)
    println(
      s"Dear ${notification.email}, you have an event ${notification.pagerEvent}"
    )
  }

  val aggregateNotificationFlow = Flow[PagerEvent]
    .conflate((e1, e2) => {
      val nInstances = e1.nInstances + e2.nInstances
      PagerEvent(
        s"You have $nInstances events that require your attention",
        new Date,
        nInstances
      )
    })
    .map(resultingEvent => Notification(onCallEngineer, resultingEvent))

  // alternative to backpressure
//  eventSource
//    .via(aggregateNotificationFlow)
//    .async
//    .to(Sink.foreach[Notification](sendEmailSlow))
//    .run()

  /*
  Slow producers: extrapolate/expand
   */

  val slowCounter = Source(Stream.from(1)).throttle(1, 1.second)
  val hungrySink = Sink.foreach[Int](println)

  val extrapolator = Flow[Int].extrapolate(element => Iterator.from(element))
  val repeater = Flow[Int].extrapolate(element => Iterator.continually(element))

  val expander = Flow[Int].expand(element => Iterator.from(element))

  slowCounter.via(repeater).to(hungrySink).run()
}
