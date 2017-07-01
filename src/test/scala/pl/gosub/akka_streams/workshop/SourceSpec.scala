package pl.gosub.akka_streams.workshop

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.FreeSpec

import scala.concurrent.{Await, Future, Promise}

// pro-tip: you need to be extra careful not to add wrong dependency (e.g. javadsl in Scala source and vice-versa)
// this is especially important when doing Scala, as Java ones will most often work, until you get into some weird
// corner case... :)

// http://doc.akka.io/docs/akka/current/scala/stream/stages-overview.html

class SourceSpec extends FreeSpec {
  "A properly materialized Source should" - {
    implicit val system = ActorSystem("SourceSpec")
    implicit val materializer = ActorMaterializer()

    "be constructed by fromIterator" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      // argument to fromIterator needs to be a function
      // you might need to hint the element type
      val source = Source.fromIterator[Int](() => fivelist.iterator)
      /*
        If the iterator perform blocking operations, make sure to run it on a separate dispatcher.
         - emits the next value returned from the iterator
         - completes when the iterator reaches its end
       */
      source.runForeach(i => println(i))(materializer)

      // Remember! if Source completes the whole stream will usually also complete
      // this might seem trivial now, but when creating custom stages
      // you need to be extra careful not to complete the stage after processing
      // a single batch of data
    }

    "be constructed with apply method" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source.apply(fivelist) // one could also Source(fivelist) :)
      /*
        Stream the values of an immutable.Seq.
         - emits the next value of the seq
         - completes when the last element of the seq has been emitted
       */
      val stream: Future[Done] = source.runForeach(i => println(i))
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
    }

    "generate a periodical repetition of an object" in {
      import scala.concurrent.duration._
      // take(n) does what it says on the tin... yes, it is a Flow :)
      val stream = Source.tick(2 seconds, 1 second, "*").take(5).runForeach(s => print(s + " "))
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      // gotcha!
      // comment out this line to see... nothing printed
      // yes! all previous examples worked by sheer luck :)
      // also, without materialized value of Future[Done] how will you wait for the end of your stream processing?
      Await.ready(stream, 10 seconds)
    }

    "emit an element refered to by a future after it completes" in {
      val stream = Source.fromFuture(Future.successful(42)).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      Await.ready(stream, 10 seconds)
    }

    "fail the stream after a future fails" in {
      val stream = Source.fromFuture(Future.failed(new RuntimeException("kaboom!"))).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      // gotcha!
      // The exception has been swallowed, the stream completed...
      // see recover/mapError in SimpleFlowSpec
      Await.ready(stream, 10 seconds)
    }

    "fail the stream with style" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      // gotcha!
      // however -> see recover/mapError in SimpleFlowSpec
      Await.ready(stream, 10 seconds)
    }

    "fail the stream with style 2" in {
      val stream = Source.failed(new RuntimeException("kaboom!")).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(res => println("stream completed with outcome: " + res))
      // gotcha!
      // however we can see that the result is a Try and peek inside for errors
      Await.ready(stream, 10 seconds)
    }

    "fail the stream with style 3" in {
      val source = Source(0 to 5).map(100 / _)
      val stream = source.runWith(Sink.fold(0)(_ + _))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(v => println("stream completed with value: " + v))
      // gotcha!
      // if you are impatient -> read here http://doc.akka.io/docs/akka/current/scala/stream/stream-error.html
      // we will cover that subject in depth when discussing recover/mapError
      Await.ready(stream, 10 seconds)
    }

    "unfold the given function" in {
      def fun(v: (Int, Int)): Option[((Int, Int), Int)] = {
        // caveat!
        // though technically possible, do not close over external state :)
        val (max, acc) = v
        if (acc <= max)
          Some((max /* some value */, acc + 1 /* fold accumulator */), acc /* propagated value */)
        else
          None
      }
      val stream = Source.unfold(5 /* initial element value */ -> 0 /* initial accumulator value */)(fun).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      Await.ready(stream, 10 seconds)
    }

    // There is an unfoldAsync version,
    // where fun is an async function (returns a Future, source emits when the Future is completed).
    // That is great, because think what would happen if you had a long running operation inside func
    // (compare with node.js).
    // On the other hand, think what happens when the Future returned by func fails... or times out...

    "unfold asynchronously the given function" in {
      def fun(v: (Int, Int)): Future[Option[((Int, Int), Int)]] = {
        // caveat!
        // though technically possible, do not close over external state :)
        val (max, acc) = v
        if (acc <= max) {
          if (acc <= max / 2) {
            Future.successful(Some((max /* some value */ , acc + 1 /* fold accumulator */ ), acc /* propagated value */))
          } else {
            Future.failed(new RuntimeException("gotcha!"))
          }
        } else {
          Future.successful(None)
        }
      }
      val stream = Source.unfoldAsync(5 /* initial element value */ -> 0 /* initial accumulator value */)(fun).runForeach(s => println(s))
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      // gotcha!
      // Yes, again a silent failure
      // Think - if this was not a source, but a flow, would it also have implications for the upstream?
      // hint: yes :)
      Await.ready(stream, 10 seconds)
    }

    // now please take a break and go to SinkSpec to learn about "materialized values"
    // which Sinks do a lot of
    // ...
    // Then come back here, to learn that "materialized value" actually is a "materialized variable"
    // which means that you not only can read from it
    // but you can also write to it.
    // ...
    // maybe :)
    "create a source with a side-channel for communication with a flow" in {
      // simple use-case, for more complex usage look at the example in
      // http://doc.akka.io/docs/akka/2.5.3/scala/stream/stream-flows-and-basics.html#flow-combine-mat

      // what is the type of this...
      val strangeSource: Source[Int, Promise[Option[Int]]] = Source.maybe[Int]
      // aha! no NotUsed, but Promise[Option[Int]] instead... how to access it?

      // don't do it this way, you'll loose the access to the side-channel
      val wrongStream: Future[Done] = Source.maybe[Int].runForeach(s => println(s))

      // toMat / Keep.both is the key
      // for more detailed discussion:
      // http://doc.akka.io/docs/akka/2.5.3/scala/stream/stream-composition.html#materialized-values
      val rightStream: (Promise[Option[Int]], Future[Done]) = Source.maybe[Int].toMat(Sink.foreach(s => println(s)))(Keep.both).run()
      val (handle, stream) = rightStream
      handle.completeWith(Future.successful(Some(42)))

      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
      // what has happened with wrongStream? did it ever run?
      Await.ready(stream, 10 seconds)
    }
  }
}
