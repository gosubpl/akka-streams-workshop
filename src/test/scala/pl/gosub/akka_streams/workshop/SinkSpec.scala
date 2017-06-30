package pl.gosub.akka_streams.workshop

import akka.Done
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.scaladsl.{Sink, SinkQueueWithCancel, Source}
import org.scalatest.FreeSpec

import scala.collection.immutable
import scala.concurrent.{Await, Future}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life

class SinkSpec extends FreeSpec {
  "A properly materialized Sink should" - {
    implicit val system = ActorSystem("SinkSpec")
    implicit val materializer = ActorMaterializer()

    // Sinks do a lot of side effects
    // That is one of most important reasons for "materialized values"
    // which can be what

    "materialize head value" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source(fivelist) // our old friend, Source.apply
      val sink: Sink[Int, Future[Int]] = Sink.head[Int]
      // the interesting bit:
      // cancels: after receiving one element
      // What implications do you think does this have for the upstream?
      val stream: Future[Int] = source.runWith(sink) // notice Future[Int] _not_ [Done]

      // now use the materializedValue
      stream.onComplete(matVal => println("stream completed, value: " + matVal))
      Await.ready(stream, 10 seconds)
    }

    "materialize last value" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source(fivelist)
      val sink: Sink[Int, Future[Int]] = Sink.last[Int]
      // the interesting bit:
      // cancels: never
      // This stage will never emit anything if the stream is infinite (never completes)
      val stream: Future[Int] = source.runWith(sink) // notice Future[Int] _not_ [Done]

      // now use the materializedValue
      stream.onComplete(matVal => println("stream completed, value: " + matVal))
      Await.ready(stream, 10 seconds)
    }

    "ignore the values" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source(fivelist)
      val sink: Sink[Any, Future[Done]] = Sink.ignore
      // cancels: never
      val stream: Future[Done] = source.runWith(sink)

      // the materialized value is just 'Done'
      stream.onComplete(matVal => println("stream completed, value: " + matVal))
      Await.ready(stream, 10 seconds)
    }

    "materialize all values as a Seq" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source(fivelist)
      val sink: Sink[Int, Future[immutable.Seq[Int]]] = Sink.seq[Int]
      // the interesting bit:
      // cancels: If too many values are collected
      // Int.MaxValue !
      val stream: Future[immutable.Seq[Int]] = source.runWith(sink)

      stream.onComplete(seq => println("stream completed, value: " + seq))
      Await.ready(stream, 10 seconds)
    }

    // let's skip foreach, already used that so many times... :)

    // if you don't understand the next example
    // read more on backpressure first
    // however, this example will be used to demonstrate the idea of backpressure during the course :)

    "materialize all values as a Queue" in {
      val fivelist: List[Int] = List(1, 2, 3, 4, 5)
      val source = Source(fivelist)
      // create SinkQueue
      val sink: Sink[Int, SinkQueueWithCancel[Int]] = Sink.queue[Int]()
      // and parametrise it, by changing the attributes of the created sink
      sink.withAttributes(Attributes.inputBuffer(1, 1)) // initial size and max size of the buffer
      // SinkQueue will ask for at most max size elements in one go
      // the interesting bit:
      // cancels: when SinkQueue.cancel is called
      // backpressures: when inputBuffer has no free space
      val sinkQueue: SinkQueueWithCancel[Int] = source.runWith(sink)

      // now the tricky part, as this does returns the queue in materialized value
      // so how do we know whether the stream has completed?

        /*
        * Method SinkQueue.pull pulls elements from stream and returns future that:
        * - fails if stream is failed
        * - completes with None in case if stream is completed
        * - completes with `Some(element)` in case next element is available from stream.
        */


      var sinkQueueNonEmpty = true
      while(sinkQueueNonEmpty) {
        Thread.sleep(500L) // manipulate sleep time / comment to see the mighty backpressure in action
        val value = for {
          value <- sinkQueue.pull()
        } yield value
        val elem = Await.result(value, 10 seconds)
        elem match {
          case Some(i) => println(i)
          case None => sinkQueueNonEmpty = false
        }
      }
    }
  }
}
