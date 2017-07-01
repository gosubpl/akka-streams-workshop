package pl.gosub.akka_streams.workshop

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.FreeSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

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
        case _                      => Supervision.Stop
      }
      implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
      val source = Source(0 to 5).map(100 / _)
      val stream = source.runWith(Sink.fold(0)(_ + _))
      stream.awaitOnCompleteAndPrint
    }

    // scan
    // for log see also http://doc.akka.io/docs/akka/current/scala/stream/stream-cookbook.html#logging-elements-of-a-stream
    "do sensible things" in {
      println("sensible")
    }

    // mapAsync / mapAsyncUnordered / scanAsync - retry/supervision strategies

  }

}

object FutureExtensions {
  implicit class FutureLocalOps[T](val fut: Future[T]) extends AnyVal {
    def awaitOnCompleteAndPrint = awaitOnComplete(returnValue => println("Stream completed with value: " + returnValue))
    def awaitOnComplete[U](f: Try[T] => U, timeout: Duration = 10 seconds): Unit = {
      fut.onComplete(f)
      Await.ready(fut, timeout)
    }
  }
}