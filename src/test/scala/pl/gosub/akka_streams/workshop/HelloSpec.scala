package pl.gosub.akka_streams.workshop

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Source}
import org.scalatest.FreeSpec

import scala.concurrent.Future

class HelloSpec extends FreeSpec {
  "An actor system and materializer should" - {
    implicit val system = ActorSystem("HelloSpec")
    implicit val materializer = ActorMaterializer()

    "runForeach with a simple source" in {
      val source: Source[Int, NotUsed] = Source(1 to 10)
      source.runForeach(i => println(i))(materializer)
    }

    "run a simple source & sink setup" in {
      val source: Source[Int, NotUsed] = Source(1 to 10)
      val sink: Sink[Int, Future[Done]] = Sink.foreach(i => println(i))
      source.runWith(sink)
    }

    "run a simple flow" in {
      val flow: Flow[Int, Int, NotUsed] = Flow.fromFunction(i => i)
      val source: Source[Int, NotUsed] = Source(1 to 10)
      val sink: Sink[Int, Future[Done]] = Sink.foreach(i => println(i))
      source.runWith(sink)
      source.via(flow).runWith(sink)
    }

    "get notification on stream termination" in {
      val source: Source[Int, NotUsed] = Source(1 to 10)
      val stream: Future[Done] = source.runForeach(i => println(i))
      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      stream.onComplete(_ => println("stream completed"))
    }

    "construct a full fledged source to sink pipeline to run" in {
      val flow: Flow[Int, Int, NotUsed] = Flow.fromFunction(i => i)
      val source: Source[Int, NotUsed] = Source(1 to 10)
      val sink: Sink[Int, Future[Done]] = Sink.foreach(i => println(i))
      val pipeline: RunnableGraph[NotUsed] = source.via(flow).to(sink) // but what about notification?
      pipeline.run()
    }

    "construct a full fledged source to sink pipeline with notification to run" in {
      val flow: Flow[Int, Int, NotUsed] = Flow.fromFunction(i => i)
      val source: Source[Int, NotUsed] = Source(1 to 10)
      val sink: Sink[Int, Future[Done]] = Sink.foreach(i => println(i))
      val source2: Source[Int, NotUsed] = source.via(flow) // Source + Flow is a Source :)
      val pipeline: RunnableGraph[Future[Done]] = source2.toMat(sink)(Keep.right) // now ok :)
      // newbie gotcha -> the .to method on Source that attaches Sink keeps the value of Source (so NotUsed) instead
      // of the one from Sink (which is the Future[Done] that is interesting for us)
      // there are reasons for that (can you think about one?), but it is puzzling for newcomers
      // Keep.right will give you the value you need :)
      // more on this later
      // btw. you could Keep.both or whatever :)
      // Bonus question, why NotUsed and not e.g. Unit ?
      // Hint: val flow: Flow[Int, Int, Cancellable] = throttler

      import scala.concurrent.ExecutionContext.Implicits.global // you should never use global context in real life
      val stream = pipeline.run()
      stream.onComplete(_ => println("stream completed again!"))
    }

    // Add here backpressure samples to illustrate the concept
  }
}
