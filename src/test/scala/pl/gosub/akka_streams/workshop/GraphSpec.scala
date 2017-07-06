package pl.gosub.akka_streams.workshop

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream._
import org.scalatest.FreeSpec

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// for graphs -> one example just flow the other with integrated source
// which will need this more complex builder
// Source, Flow, etc BidiFlow.fromGraph(...)
class GraphSpec extends FreeSpec {
  import FutureExtensions._

  "A simple Graph should" - {
    implicit val system = ActorSystem("GraphSpec-1")
    implicit val materializer = ActorMaterializer()

    "use broadcast and merge to divide the work" in {
      val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val in = Source(0 to 9)
        val out = Sink.foreach(println)

        val bcast = builder.add(Broadcast[Int](2))
        val merge = builder.add(Merge[Int](2))

        val f1, f3 = Flow[Int].map(_ + 0) // do nothing
        val f2 = Flow[Int].map(_ + 10)
        val f4 = Flow[Int].map(_ + 20)

        in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out
                    bcast ~> f4 ~> merge
        ClosedShape
      })
      // no control over stream :( how did it happen?
      val stream: NotUsed = graph.run()
      Thread.sleep(1000)
    }

    // you need to be extra careful, otherwise you will end up with incomprehensible errors :)
    "use broadcast and merge to create a Source" in {
      val graphSource: Source[Int, NotUsed] = Source.fromGraph(GraphDSL.create() { implicit builder  =>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val in = Source(0 to 9)

        val bcast = builder.add(Broadcast[Int](2))
        val merge = builder.add(Merge[Int](2))

        val f1, f3 = Flow[Int].map(_ + 0) // do nothing
        val f2 = Flow[Int].map(_ + 10)
        val f4 = Flow[Int].map(_ + 20)

        // needs to be added to the builder
        // comment out this line and change outflow to f3 everywhere to see what kinds of error I am thinking about :)
        val outflow = builder.add(f3)

        in ~> f1 ~> bcast ~> f2 ~> merge ~> outflow
                    bcast ~> f4 ~> merge
        // expose the outlet
        SourceShape(outflow.out)
      })

      val stream = graphSource.runWith(Sink.foreach(println))

      // much better now!
      stream.awaitOnCompleteAndPrint
    }

    "use broadcast and merge to create a Flow" in {
      val graphFlow: Flow[Int, Int, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder  =>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val bcast = builder.add(Broadcast[Int](2))
        val merge = builder.add(Merge[Int](2))

        val f3 = Flow[Int].map(_ + 0) // do nothing
        val f2 = Flow[Int].map(_ + 10)
        val f4 = Flow[Int].map(_ + 20)

        // needs to be added to the builder to be exposable in the shape below
        val outflow = builder.add(f3)

        bcast ~> f2 ~> merge ~> outflow
        bcast ~> f4 ~> merge

        // expose the inlet and the outlet
        // as bcast is already in the builder, no special tricks are needed
        FlowShape(bcast.in, outflow.out)
      })

      val stream = Source(0 to 9).via(graphFlow).runWith(Sink.foreach(println))

      // much better now!
      stream.awaitOnCompleteAndPrint
    }

    // various other shapes are possible, look them up in the manual
    // http://doc.akka.io/docs/akka/current/scala/stream/stream-graphs.html#predefined-shapes
    // or borrow directly from akka source - e.g. BidiShape :)
    // https://github.com/gosubpl/akka-online/blob/master/src/main/scala/pl/gosub/akka/online/BloomFilterCrossMatStage.scala
    // you can also create your own shapes (see CustomGraphSpec.scala)
    // https://github.com/gosubpl/akka-online/blob/master/src/main/scala/pl/gosub/akka/online/SuffixTreeTripodMatStage.scala
    // TripodShape is a bit of a joke, as FanInShape2 has the same shape, and shape is just a shape ;)

    // so far so good, but let's say we would like to actually get the materialized returned from the stage
    // e.g. to have Sink.fold as the last step of the stage
    // there is then this clever idiom, slightly hinted at in
    // http://doc.akka.io/docs/akka/current/scala/stream/stream-graphs.html#accessing-the-materialized-value-inside-the-graph
    // thanks hseeberger for pointing it out to me and explaining in detail :)

    "be able to create a Sink in dsl to use its materialized value" in {
      // be careful, simplified form Sink.fold(0)(_ + _) seems not to work here
      // and you'll get a lot of weird errors, as the problem is the wrong type for myCreatedSink
      // because of local type inference not assigning the right type to T in the Sink.fold constructor
      val graph = RunnableGraph.fromGraph(GraphDSL.create(Sink.fold(0)((acc: Int, el: Int) => acc + el)) { implicit builder => myCreatedSink =>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val in = Source(0 to 9)

        val bcast = builder.add(Broadcast[Int](2))
        val merge = builder.add(Merge[Int](2))

        val f1, f3 = Flow[Int].map(_ + 0) // do nothing
        val f2 = Flow[Int].map(_ + 10)
        val f4 = Flow[Int].map(_ + 20)

        in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> myCreatedSink
                    bcast ~> f4 ~> merge
        ClosedShape
      })
      // now we are able to control the stream :)
      val stream: Future[Int] = graph.run()
      stream.awaitOnCompleteAndPrint
    }
  }

  // see here: http://doc.akka.io/docs/akka/current/scala/stream/stream-graphs.html#graph-cycles-liveness-and-deadlocks
  // for discussion of deadlocks in graphs, merge.preferred and application of buffers with relevant overflow strategies to deal with those
  // also akka-stream-testkit helps us with finding such issues

  // as combinators we can use following stages
  // fan-out (like broadcast):
  // http://doc.akka.io/docs/akka/current/scala/stream/stages-overview.html#fan-out-stages
  // fan-in (like merge):
  // http://doc.akka.io/docs/akka/current/scala/stream/stages-overview.html#fan-in-stages

  // there is also a simplified syntax for creating graphs using those stages
  // http://doc.akka.io/docs/akka/current/scala/stream/stream-graphs.html#combining-sources-and-sinks-with-simplified-api

}
