package pl.gosub.akka_streams.workshop

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest.FreeSpec

import scala.concurrent.Future

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
  }
}
