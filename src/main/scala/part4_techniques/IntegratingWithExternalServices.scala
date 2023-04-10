package part4_techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import java.util.Date
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
object IntegratingWithExternalServices extends App {
  implicit val system = ActorSystem("integrating-with-external-services")
  // import system.dispatcher // not recommended in practice for mapAsync
  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")
  def genericExtService[A, B](element: A): Future[B] = ???

  // example: PagerDuty
  case class PagerEvent(application: String, description: String, date: Date)
  val eventSource = Source(
    List(
      PagerEvent("akka-infra", "infra broke", new Date),
      PagerEvent(
        "fast-data-pipeline",
        "illegal elements in data pipeline",
        new Date
      ),
      PagerEvent("akka-infra", "A service stopped responding", new Date),
      PagerEvent("super-frontend", "A button does not work", new Date)
    )
  )

  class PagerActor extends Actor with ActorLogging {
    private val engineers = List("raunak", "john", "ladygaga")
    private val email = Map(
      "raunak" -> "raunak@gmail.com",
      "john" -> "john@gmail.com",
      "ladygaga" -> "ladygaga@gmail.com"
    )

    private def processEvent(pagerEvent: PagerEvent) = {
      val engineerIndex =
        (pagerEvent.date.toInstant.getEpochSecond / (24 * 3600)) % engineers.length
      val engineer = engineers(engineerIndex.toInt)
      val engineerEmail = email(engineer)

      // page the engineer
      log.info(
        s"Sending engineer: $engineerEmail a high priority notification: $pagerEvent"
      )

      Thread.sleep(1000)
      engineerEmail
    }

    override def receive: Receive = { case pagerEvent: PagerEvent =>
      sender() ! processEvent(pagerEvent)
    }
  }
  import akka.pattern.ask
  implicit val timeout = Timeout(3.seconds)
  val infraEvents = eventSource.filter(_.application == "akka-infra")
  val pagerActor = system.actorOf(Props[PagerActor], "pagerActor")
  val alternativePagedEngineerEmails =
    infraEvents.mapAsync(parallelism = 4)(event =>
      (pagerActor ? event).mapTo[String]
    )
  val pagedEmailsSink = Sink.foreach[String](email =>
    println(s"Succesfully sent notification to $email")
  )

  alternativePagedEngineerEmails.to(pagedEmailsSink).run()

  // Do not confuse mapSync with async (Async boundary)
}
