package pl.gosub.akka_streams.workshop

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, ThrottleMode}
import org.scalatest.FreeSpec

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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
  "A Flow should in the case of recovery" - {
    implicit val system = ActorSystem("SimpleFlowSpec")
    implicit val materializer = ActorMaterializer()

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

    "switch the Source in the failing Stream using recoverWith" in {
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
  }

    // more on Error handling in AsyncFlowSpec
    // and http://doc.akka.io/docs/akka/current/scala/stream/stream-error.html
    // please go to AsyncFlowSpec now and then return here :)

    // ...

    // ok, welcome back after the break :)
  "A Flow to show how backpressure works" - {
    implicit val system = ActorSystem("SimpleFlowSpec")
    implicit val materializer = ActorMaterializer()

    // throttle (more on backpressure) & intersperse
    // plus Source.queue

    // Remember the Tick example? We were getting an element second or so
    // can we somehow achieve the same effect with a Flow element?
    // Sure, use throttle :)
    "use throttle to shape the traffic from a Source" in {
      val source = Source(1 to 5)
      // shape the traffic
      // 1 element per 200 millis, 0 elements max saved for later (burst rate / bucket capacity)
      val stream = source.throttle(1, Duration(200, "millisecond"), 0, ThrottleMode.shaping)
        .runForeach(s => println(s))
      stream.onComplete(_ => println("Stream completed"))
      Await.ready(stream, 10 seconds)
      // 1 element per 200 millis, 2 elements max saved for later (burst rate / bucket capacity)
      val stream2 = source.throttle(1, Duration(200, "millisecond"), 2, ThrottleMode.shaping)
        .runForeach(s => println(s))
      stream2.onComplete(_ => println("Stream completed"))
      Await.ready(stream2, 10 seconds)
    }

    "demo the maxBurst parameter in Throttle" in {
      val source = Source(1 to 10)
      // 4 elements per 800 millis, up to 5 in one go
      val stream = source.throttle(4, Duration(2000, "millisecond"), 5, ThrottleMode.shaping)
        .runForeach(s => println(s))
      stream.onComplete(_ => println("Stream completed"))
      Await.ready(stream, 10 seconds)
    }

    "use throttle to demo the enforcing mode" in {
      val source = Source(1 to 5)
      // shape the traffic
      // 1 element per 200 millis, 0 elements max saved for later (burst rate / bucket capacity)
      val stream = source.throttle(1, Duration(200, "millisecond"), 2, ThrottleMode.enforcing)
        .runForeach(s => println(s))
      stream.onComplete(res => println("Stream completed with result: " + res))
      Await.ready(stream, 10 seconds)
      val stream2 = source
        .throttle(1, Duration(300, "millisecond"), 0, ThrottleMode.shaping)
        // in enforcing throttle, maxBurst must be > 0
        // it is actually the number of elements after which the throttle condition is checked
        // if it is 0 then Throttle will fail immediately...
        .throttle(2, Duration(400, "millisecond"), 1, ThrottleMode.enforcing)
        .runForeach(s => println(s))
      stream2.onComplete(res => println("Stream completed with result: " + res))
      Await.ready(stream2, 10 seconds)
    }

    "show how Source.queue interacts with Throttle in backpressure mode" in {
      // backpressure strategy says - if downstream is too slow, wait on accepting element in offer()
      // until the stream can accept additional elements
      val source = Source.queue[Int](5, OverflowStrategy.backpressure)
      // akka-streams idiom, need to do it this way, instead of runForeach to get the queue out as materialized value
      val sourceQueue = source.throttle(1, 500 millis, 1, ThrottleMode.shaping)
        .to(Sink.foreach(println))
        .run()
      // be careful, for offer returns a Future :)
      var i = 0
      while (i < 10) {
        i += 1
        Await.ready(sourceQueue.offer(i), 10 seconds)
      }
      // all elements (maybe except the last :) ) processed, let's conclude the stream (we could also fail() the stream)
      Thread.sleep(1000) // look at what offer() returns to understand why there might be a race here :)
      sourceQueue.complete()
      // for queues .onComplete will not work
      // you need to use watch completion
      val done = sourceQueue.watchCompletion()
      done.onComplete(_ => println("Stream completed"))
      Await.ready(done, 10 seconds)
      // another race - wait for the last number to be printed ...
      // can complete travel faster downstream than elements? maybe... ;)
      Thread.sleep(1000)
    }

    "show how Source.queue interacts with Throttle in dropNew mode" in {
      // dropNew strategy says - if downstream is too slow, drop all new elements
      // coming via offer()
      // in this case, it means only elements 1-5 will be processed ... or will they?
      val source = Source.queue[Int](5, OverflowStrategy.dropNew)
      // 1 in maxBurst below will give you elements 1-7, 2 will give you 1-8 etc...
      // 0 should probably give you 1-6 :)
      val sourceQueue = source.throttle(1, 500 millis, 1, ThrottleMode.shaping)
        .to(Sink.foreach(println))
        .run()
      var i = 0
      while (i < 10) {
        i += 1
        Await.ready(sourceQueue.offer(i), 10 seconds)
      }
      // all elements (maybe except the last :) ) processed, let's conclude the stream (we could also fail() the stream)
      Thread.sleep(1000)
      sourceQueue.complete()
      val done = sourceQueue.watchCompletion()
      done.onComplete(_ => println("Stream completed"))
      Await.ready(done, 10 seconds)
      // another possible race - so wait to check if we have really printed all elements
      Thread.sleep(1000)
    }

    // Now go to the SinkSpec Source.queue example to look at the inputBuffer setting,
    // global setting is akka.stream.materializer.max-input-buffer-size (= 16).
    // You can also set settings as .withInputBuffer(initialSize = 64, maxSize = 64) at the materializer level.
    // Please remember that there is lots of fusing, which means that every materialized island has its own buffers
    // (to be precise: one inputBuffer per each input slot)
    // but the stages inside an island are connected directly, without any buffers inbetween,
    // hence the usual explanation of how backpressure works (pull/push) is only an approximation.
    // Also - there is some good advice in the manual to set inputBuffer to 1/1 in case of any performance issues.
    // Shape of the islands depend on particular materializer, and this can change (e.g. at 2.0-M2 or between 2.4 and 2.5).
    // Since 2.5 the setting telling Akka not to do fusing is ignored :) .
    // You can always request your stage to be separate by adding .async parameter.

    // now on to Buffer and Detach to see those ideas implemented in Flow elements
    // see for more: http://doc.akka.io/docs/akka/current/scala/stream/stream-rate.html

    // We can have an explicit buffer in akka-streams, to adjust the difference between fast producer and slow consumer
    // bear in mind, that if the difference is permanent, then we need to do something about superfluous elements
    // there is a couple of OverflowStrategies there
    "demo the dropTail overflow strategy using Buffer" in {
      val source = Source(1 to 10)
      // 4 elements per 800 millis, up to 5 in one go
      val stream = source
        .throttle(1, 100.millis, 0, ThrottleMode.shaping)
        // without buffer in between, we would just get numbers 1-10, one each 500 millis
        // now with a buffer... in between
        // you can test various strategies here
        .buffer(5, OverflowStrategy.dropTail)
        .throttle(1, 500.millis, 0, ThrottleMode.shaping)
        .runForeach(s => println(s))
      stream.onComplete(_ => println("Stream completed"))
      Await.ready(stream, 10.seconds)
    }

    // detach is like buffer(1, OverflowStrategy.backpressure)
    "demo the detach stage" in {
      val source = Source(1 to 10)
      // 4 elements per 800 millis, up to 5 in one go
      val stream = source
        .throttle(1, 100.millis, 0, ThrottleMode.shaping)
        .detach
        .throttle(1, 500.millis, 0, ThrottleMode.shaping)
        .runForeach(s => println(s))
      stream.onComplete(_ => println("Stream completed"))
      Await.ready(stream, 10.seconds)
    }

    // limit / limitWeighted - limit errors the stage if there is more than limit elements to be processed by the
    // stream - this is useful if e.g. stream processes data to be later collected into some result
    // for further processing and we want to prevent unboundedness as soon as possible (close to origin/Source)
    // see also http://doc.akka.io/docs/akka/current/scala/stream/stream-cookbook.html#draining-a-stream-to-a-strict-collection
    "demo the limit stage" in {
      val source = Source(1 to 10)
      val stream = source
        .limit(100)
        .runForeach(s => println(s))
      stream.onComplete(res => println("Stream completed with result: " + res))
      Await.ready(stream, 10.seconds)
      val stream2 = source
        .limit(5)
        .runForeach(s => println(s))
      // please note that exception message is off-by-one:
      // Stream completed with result: Failure(akka.stream.StreamLimitReachedException: limit of 5 reached)
      stream2.onComplete(res => println("Stream completed with result: " + res))
      Await.ready(stream2, 10.seconds)
    }

  }
}
