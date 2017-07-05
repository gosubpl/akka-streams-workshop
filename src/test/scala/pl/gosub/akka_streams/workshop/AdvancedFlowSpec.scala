package pl.gosub.akka_streams.workshop

import java.nio.file.FileSystems

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream._
import akka.stream.testkit.TestSubscriber
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import org.scalatest.FreeSpec

import scala.concurrent.{Future, TimeoutException}
import scala.util.Random
import scala.util.control.NoStackTrace

class AdvancedFlowSpec extends FreeSpec {

  import FutureExtensions._

  // rate matching (conflate, ....)


  // As we are entering the world of more complex stages,
  // we could use some tricks from the akka-stream-testkit
  // which is what you can and should use to test your streams
  // instead of simple println debugging :)
  "A properly working testkit should" - {
    implicit val system = ActorSystem("AdvancedFlowSpec-1")
    implicit val materializer = ActorMaterializer()

    "let us test a Source using TestSink" in {
      val sourceUnderTest = Source(1 to 4).filter(_ % 2 == 0).map(_ * 2)

      sourceUnderTest
        .runWith(TestSink.probe[Int])
        .request(2)
        .expectNext(4, 8)
        .expectComplete()
    }

    "let us test a Sink using TestSource" in {
      val sinkUnderTest = Sink.cancelled

      TestSource.probe[Int]
        .toMat(sinkUnderTest)(Keep.left)
        .run()
        .expectCancellation()
    }
  }

  "IO stages in a system with custom blocking dispatcher should" - {
    // Read more on IO stages here
    // http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-io.html
    val config = ConfigFactory.parseString(
      s"""
         |  custom-blocking-io-dispatcher {
         |      type = "Dispatcher"
         |      executor = "thread-pool-executor"
         |      throughput = 1
         |
         |      thread-pool-executor {
         |        fixed-pool-size = 16
         |      }
         |    }
       """.stripMargin)
    implicit val system = ActorSystem("AdvancedFlowSpec-2", config)
    implicit val materializer = ActorMaterializer()

    // by default all IO stages are run on default-blocking-io-dispatcher
    // which is a part of standard AkkaSystem config
    // Never run IO on default-dispatcher
    "run on a custom dispatcher" in {
      val fsPath = this.getClass.getResource("/multiline_text.txt").getPath
      val fs = FileSystems.getDefault
      val stream = FileIO.fromPath(fs.getPath(fsPath))
        .withAttributes(ActorAttributes.dispatcher("custom-blocking-io-dispatcher"))
        .map(_.utf8String)
        .runWith(Sink.foreach(println))
      stream.awaitOnCompleteAndPrint
    }
  }

  "A Flow with timer driven stages should" - {
    implicit val system = ActorSystem("AdvancedFlowSpec-3")
    implicit val materializer = ActorMaterializer()

    "enable groupedWithin" in {
      val stream = Source(1 to 100).throttle(1, 10.millis, 1, ThrottleMode.shaping)
        .map { elem =>
          // poor man's delay()
          // but can delay by a random time
          // should run on dedicated dispatcher
          Thread.sleep(Random.nextInt(10))
          elem
        }.groupedWithin(20, 200.millis)
        .runWith(Sink.foreach(println))
      stream.awaitOnCompleteAndPrint
    }
    // We have also: takeWithin, dropWithin, groupedWeightedWithin, initialDelay and delay
    // this can be used to implement windowing in a simple way
    // also moving averages ;)

    "fail stage if not completed on time with completionTimeout" in {
      val stream = Source(1 to 100).throttle(1, 10.millis, 1, ThrottleMode.shaping)
        .completionTimeout(25.millis)
        .runWith(TestSink.probe[Int])
        .requestNext(1).requestNext(2)
        .expectError()
    }
    // also initialTimeout, idleTimeout, backpressureTimeout
    // check also keepAlive (emit when the stream was idle for too long)
  }

  "A Stream with SubStreams should" - {
    implicit val system = ActorSystem("AdvancedFlowSpec-4")
    implicit val materializer = ActorMaterializer()

    "group them by and then merge again" in {
      val stream = Source(1 to 100)
        .groupBy(10, _ % 10)
        .map(v => (v % 10, v)).async
        .mergeSubstreams
        .runWith(Sink.foreach(println))
      stream.awaitOnCompleteAndPrint
    }
    // you could also concatSubstreams , whatever that means ;)
    // nice example of using SubStreams can be found
    // here: https://softwaremill.com/windowing-data-in-akka-streams/
  }

  "A Flow with unmatched rates should" - {
    implicit val system = ActorSystem("AdvancedFlowSpec-5")
    implicit val materializer = ActorMaterializer()

    "be fixable with conflate for faster producer" in {
      val fastSource = Source(1 to 100).throttle(1, 10.millis, 1, ThrottleMode.shaping)
      val slowFlow = Flow[Int].throttle(1, 100.millis, 1, ThrottleMode.shaping)
      val stream = fastSource.conflate(Keep.right).via(slowFlow).runWith(Sink.foreach(println))
      stream.awaitOnCompleteAndPrint
      val stream2 = fastSource.conflate(_ + _).via(slowFlow).runWith(Sink.foreach(println))
      stream2.awaitOnCompleteAndPrint
      // this will count the elements in each package
      val stream3 = fastSource.conflateWithSeed(seed = (_) => 1)((count, _) => count + 1).via(slowFlow).runWith(Sink.foreach(println))
      stream3.awaitOnCompleteAndPrint
      // you can use conflateWithSeed to calculate the average of the package
      // you can also use buffer, as already seen
    }

    "be fixable with expand for faster consumer" in {
      val slowSource = Source(1 to 5).throttle(1, 100.millis, 1, ThrottleMode.shaping)
      val fastFlow = Flow[Int].throttle(1, 10.millis, 1, ThrottleMode.shaping)
      val stream = slowSource
        .expand(Iterator.continually(_))
        .via(fastFlow).runWith(Sink.foreach(println))
      stream.awaitOnCompleteAndPrint
    }
  }

  "A Flow with KillSwitch should" - {
    implicit val system = ActorSystem("AdvancedFlowSpec-6")
    implicit val materializer = ActorMaterializer()
    "be stoppable while executing" in {
      val exception = new Exception("Exception from KillSwitch") with NoStackTrace
      val stream: RunnableGraph[(UniqueKillSwitch, Future[Done])] =
        Source(1 to 10).throttle(1, 100.millis, 1, ThrottleMode.shaping).map(_ * 2)
      .viaMat(KillSwitches.single)(Keep.right) // we need to propagate the KillSwitch materialized variable (value)
      .toMat(Sink.foreach(println))(Keep.both) // we need to Keep the KillSwitch mat. value too!

      val (switch, fut) = stream.run() // golang style ;O
      switch.abort(exception)
      fut.awaitOnCompleteAndPrint
    }
  }
}