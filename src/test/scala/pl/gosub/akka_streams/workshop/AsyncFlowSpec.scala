package pl.gosub.akka_streams.workshop

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, ActorMaterializerSettings, Supervision}
import org.scalatest.FreeSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Random, Try}

// Error handling details here: http://doc.akka.io/docs/akka/current/scala/stream/stream-error.html
// Some examples adapted from there

class AsyncFlowSpec extends FreeSpec {

  import FutureExtensions._

  "A Flow with custom materializer should" - {
    implicit val system = ActorSystem("SimpleFlowSpec")

    // now we will be having customized materializers everywhere

    "finish with failure for default materializer" in {
      implicit val materializer = ActorMaterializer()
      val source = Source(0 to 5).map(100 / _)
      val stream = source.runWith(Sink.fold(0)(_ + _))
      stream.awaitOnComplete(v => println("Stream completed with value: " + v))
    }

    "finish without failure for materializer with custom decider" in {
      val decider: Supervision.Decider = {
        case _: ArithmeticException => Supervision.Resume
        case _ => Supervision.Stop
      }
      implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
      val source = Source(0 to 5).map(100 / _)
      val stream = source.runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint
    }
  }

  "A Flow with standard materializer should" - {
    implicit val system = ActorSystem("SimpleFlowSpec")
    implicit val materializer = ActorMaterializer()

    "finish without failure for custom decider attached at flow level" in {
      val decider: Supervision.Decider = Supervision.resumingDecider // always Resume
      val source: Source[Int, NotUsed] = Source(0 to 10)
      val flow: Flow[Int, Int, NotUsed] = Flow[Int].map(100 / _).withAttributes(ActorAttributes.supervisionStrategy(decider))
      val stream = source.via(flow).runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint
    }

    // what strategies can we choose?
    /*
      Stop - The stream is completed with failure. [Default decider]
      Resume - The element is dropped and the stream continues.
      Restart - The element is dropped and the stream continues after restarting the stage.
                Restarting a stage means that any accumulated state is cleared.
                This is typically performed by creating a new instance of the stage.

      difference between Resume and Restart can be seen e.g. in Scan operation
     */

    // scan
    "adjust strategy for flow using Scan depending on decider" in {
      // what is Scan ? it is online fold, i.e.
      Source(1 to 7).scan(1)(_ * _).log("elementStream") // http://doc.akka.io/docs/akka/current/scala/stream/stages-overview.html#log
        .withAttributes(ActorAttributes.logLevels(onElement = Logging.ErrorLevel)) // log each element at ERROR level
        .runWith(Sink.foreach(println)).awaitOnComplete(_ => println("Scan run completed"))

      // now let's see how it behaves with default decider (that is Stop)
      val source: Source[Int, NotUsed] = Source(0 to 20)
      val flow: Flow[Int, Int, NotUsed] = Flow[Int].map {
        i =>
          if (i % 10 == 0) {
            throw new IllegalArgumentException("don't like numbers divisble by 10")
          } else {
            100 / i
          }
      }.scan(0)(_ + _)
      val stream = source.via(flow).runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint

      // with resuming decider
      val flow2 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      val stream2 = source.via(flow2)
        .runWith(Sink.foreach(println))
      stream2.awaitOnCompleteAndPrint

      // with restarting decider
      val flow3 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
      val stream3 = source.via(flow3)
        .runWith(Sink.foreach(println))
      stream3.awaitOnCompleteAndPrint

      // why no difference? because Map was first impacted by the resume strategy...
      // so Scan is getting only nice numbers... as we will see later on
    }

    "adjust strategy for Scan stage depending on decider" in {
      // now let's see how it behaves with default decider (that is Stop)
      val source: Source[Int, NotUsed] = Source(0 to 20)
      val flow: Flow[Int, Int, NotUsed] = Flow[Int].scan(0) {
        (acc, el) =>
          if (el % 10 == 0) {
            throw new IllegalArgumentException("don't like numbers divisble by 10")
          } else {
            acc + (100 / el)
          }
      }
      val stream = source.via(flow).runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint

      // with resuming decider
      val flow2 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      val stream2 = source.via(flow2)
        .runWith(Sink.foreach(println))
      stream2.awaitOnCompleteAndPrint

      // with restarting decider
      val flow3 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
      val stream3 = source.via(flow3)
        .runWith(Sink.foreach(println))
      stream3.awaitOnCompleteAndPrint
    }

    "compare the internals of both cases with log" in {
      val source: Source[Int, NotUsed] = Source(0 to 5)
      val flowA: Flow[Int, Int, NotUsed] = Flow[Int].map {
        i =>
          if (i % 5 == 0) {
            throw new IllegalArgumentException("don't like numbers divisble by 5")
          } else {
            100 / i
          }
      }.log("element stream 1").scan(0)(_ + _).log("scan stream 1")

      val flowB: Flow[Int, Int, NotUsed] = Flow[Int].scan(0) {
        (acc, el) =>
          if (el % 10 == 0) {
            throw new IllegalArgumentException("don't like numbers divisble by 10")
          } else {
            acc + (100 / el)
          }
      }.log("scan stream 2")

      // be extra careful to set attributes only in one place
      // additional attributes to be added using the .and combinator :)
      // otherwise they will override

      // with restarting decider
      val flow1 = flowA.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider)
        .and(ActorAttributes.logLevels(onElement = Logging.ErrorLevel)))
      val stream1 = source.via(flow1)
        .runWith(Sink.ignore)
      stream1.awaitOnCompleteAndPrint

      // with restarting decider
      val flow2 = flowB.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider)
        .and(ActorAttributes.logLevels(onElement = Logging.ErrorLevel)))
      val stream2 = source.via(flow2)
        .runWith(Sink.ignore)
      stream2.awaitOnCompleteAndPrint
    }

    // mapAsync / mapAsyncUnordered / scanAsync - retry/supervision strategies
    "use mapAsync to asynchronously map things" in {
      val source: Source[Int, NotUsed] = Source(0 to 10)
      // change to mapAsyncUnordered to see the difference :)
      // 11 means that all elements will be processed in parallel
      val flow = Flow[Int].mapAsync(11) {
        i =>
          if (i % 2 == 0)
            Future.failed[Int](new IllegalArgumentException("don't accept numbers divisible by 2"))
          else {
            Future {
              Thread.sleep(Random.nextInt(100))
              i
            }
          }
      }
      val stream = source.via(flow).runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint

      // with resuming decider
      val flow2 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      val stream2 = source.via(flow2)
        .runWith(Sink.foreach(println))
      stream2.awaitOnCompleteAndPrint

      // with restarting decider
      val flow3 = flow.withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
      val stream3 = source.via(flow3)
        .runWith(Sink.foreach(println))
      stream3.awaitOnCompleteAndPrint
    }
  }
}

object FutureExtensions {

  implicit class FutureLocalOps[T](val fut: Future[T]) extends AnyVal {
    def awaitVeryLongAndPrint = awaitOnComplete(returnValue => println("Stream completed with value: " + returnValue), 100.seconds)

    def awaitOnCompleteAndPrint = awaitOnComplete(returnValue => println("Stream completed with value: " + returnValue))

    def awaitOnComplete[U](f: Try[T] => U, timeout: Duration = 10 seconds): Unit = {
      fut.onComplete(f)
      Await.ready(fut, timeout)
    }
  }

}