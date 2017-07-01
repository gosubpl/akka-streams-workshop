package pl.gosub.akka_streams.workshop

import java.util.Locale

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, SinkQueueWithCancel, Source}
import akka.stream.{ActorMaterializer, Attributes}
import org.scalatest.FreeSpec

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SimpleFlowSpec extends FreeSpec {
  "A properly materialized simple Flow should" - {
    implicit val system = ActorSystem("SimpleFlowSpec")
    implicit val materializer = ActorMaterializer()

    // Sinks do a lot of side effects
    // That is one of most important reasons for "materialized values"
    // which can be what

    "be able to map in many ways" in {
      val fiveList: List[Int] = List(1, 2, 3, 4, 5)
      val source1 = Source(fiveList)
      val stream1 = source1.map(_ * 2).runWith(Sink.foreach(i => println(i)))
      stream1.onComplete(_ => println("stream completed"))
      Await.ready(stream1, 10 seconds)

      val stringList: List[String] = List("Hello", "cruel", "world")
      val source2 = Source(stringList)
      // watch out for de_DE and tr_TR :)
      val stream2 = source2.map(_.toUpperCase()).runWith(Sink.foreach(s => println(s)))
      stream2.onComplete(_ => println("stream completed"))
      Await.ready(stream2, 10 seconds)

      val source3 = Source(stringList)
      // mapConcat will do bad things to your strings...
      // why am I telling about that? there is statefulMapConcat
      // but no statefulMap, so whenever you need state, you frequently end up with that problem
      val stream3 = source3.mapConcat(_.toUpperCase()).runWith(Sink.foreach(s => println(s)))
      stream3.onComplete(_ => println("stream completed"))
      Await.ready(stream3, 10 seconds)

      val source4 = Source(stringList)
      // the solution, copied from akka-http StreamUtils.statefulMap, is to append Nil :)
      // see also http://doc.akka.io/docs/akka/current/scala/stream/stream-cookbook.html#flattening-a-stream-of-sequences
      val stream4 = source4.mapConcat(_.toUpperCase() :: Nil).runWith(Sink.foreach(s => println(s)))
      stream4.onComplete(_ => println("stream completed"))
      Await.ready(stream4, 10 seconds)
    }

    // other stages of interest:
    // filter, filterNot, collect (filter + map, takes partial function)
    // grouped, sliding
    // fold, foldAsync, reduce (similar to Sink)
    // scan, scanAsync (sort of online fold, see below)
    // drop, take, dropWhile, takeWhile
    // recover, recoverWith, recoverWithRetries, mapError
    // detach (act like a buffer of size 1)
    // limit, limitWeighted (limit is limitWeighted with constant weight function of 1)
    // throttle
    // intersperse
    // log (that is cool, isn't it?)


    // not so easy to understand simple stages to cover:
    // recover / recoverWith - continue the exception handling thread from the source thing
    // also throw an exception -> so this is for actually making stream not fail silently
    // cool usage in akka-http decodeRequestWith
    // also recoverWithRetries is used in a 100 CONTINUE error case in HTTP processing - do you see it is a good match?
    // not a very well known stage :) also a part of setup people tend to use less -> dynamically created streams
    // very much possible with the new 2.5 materializer
    "prevent failure in a Stream with recover" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).recover {
        case e: Exception => new RuntimeException("Stream failed because of an exception")
        case _ => // do nothing
      }.runForeach(s => println(s))

      stream.onComplete(_ => println("stream completed"))
      // Now you can see what has happened
      // even if you do not analyse the underlying try :)
      // However, the exception will still not be propagated outside the program
      // and as you can see from the next example, the stream is now successful
      // This use case is similar to mapError
      Await.ready(stream, 10 seconds)
    }

    "prevent failure in a Stream with recover 2" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).recover {
        // we can even copy the original exception
        case e: Exception => e
      }.runForeach(s => println(s))

      // as you can see here the stream is now successful!
      stream.onComplete(res => println("stream completed with outcome: " + res))
      Await.ready(stream, 10 seconds)
    }

    "log failure in a Stream with recover" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).recover {
        // notice small difference here, we throw instead of mapping
        case e: Exception => throw new RuntimeException("Stream failed because of an exception")
        case _ => // do nothing
      }.runForeach(s => println(s))

      stream.onComplete(res => println("stream completed with result: " + res))
      // The exception will still not be propagated outside the program
      // but contrary to the previous example, the stream is now a failure (see inside the Try)
      // This use case is similar to mapError
      Await.ready(stream, 10 seconds)
    }

    "terminate the Stream gracefully with recover" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).recover {
        // we map an error to some meaningful value
        case _ => "TERMINATION TOKEN"
      }.runForeach(s => println(s))

      stream.onComplete(v => println("stream completed with outcome: " + v))
      // you see the custom element supplied
      // this is what mapError cannot do
      // Also, the stream is a success now
      Await.ready(stream, 10 seconds)
    }

    "map failure in a Stream with mapError" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).mapError {
        case e: Exception => new IllegalArgumentException("something different")
      }.runForeach(s => println(s))

      stream.onComplete(res => println("stream completed with: " + res))
      // now you can see what has happened
      // however, contrary to recover you cannot supply your own element - you can only re-throw or throw a different exception
      // and the stream is still a failure
      Await.ready(stream, 10 seconds)
    }

    "log failure in a Stream with mapError" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).mapError {
        case e: Exception => throw new IllegalArgumentException("something different")
      }.runForeach(s => println(s))

      stream.onComplete(res => println("stream completed with: " + res))
      // now you can see what has happened
      // contrary to the recover case, the stream is still a failure
      // but as in the recover case the error gets logged when thrown inside the stage
      Await.ready(stream, 10 seconds)
    }

    "swithch the Source in the failing Stream using recoverWith" in {
      val alternativeSource = Source(List(1, 2, 3))
      // recoverWith is deprecated
      // you should use recoverWithRetries(-1, ...)
      // which is essentially what recoverWith meant until the deprecation :)
      val stream = Source.failed(new RuntimeException("kaboom!")).recoverWith {
        case _ => alternativeSource
      }.runForeach(s => println(s))

      stream.onComplete(_ => println("stream completed"))
      // you see the elements from alternative source supplied
      Await.ready(stream, 10 seconds)
    }

    // expand here more on http://doc.akka.io/docs/akka/current/scala/stream/stream-error.html
    // or maybe in a separate spec :)

    // scan & log
    // see also http://doc.akka.io/docs/akka/current/scala/stream/stream-cookbook.html#logging-elements-of-a-stream

    // mapAsync / mapAsyncUnordered / scanAsync - retry/supervision strategies

    // throttle (more demo backpressure) & intersperse

    // detach, buffer - backpressure and drop strategies
    // see for more: http://doc.akka.io/docs/akka/current/scala/stream/stream-rate.html

    // limit / limitWeighted - limit errors the stage if there is more than limit elements to be processed by the
    // stream - this is useful if e.g. stream processes data to be later collected into some result
    // for further processing and we want to prevent unboundedness as soon as possible (close to origin/Source)
    // see also http://doc.akka.io/docs/akka/current/scala/stream/stream-cookbook.html#draining-a-stream-to-a-strict-collection

  }
}
