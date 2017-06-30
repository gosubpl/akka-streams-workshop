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
      // but no statefulMap, so if you need state, you frequently end up with that problem
      val stream3 = source3.mapConcat(_.toUpperCase()).runWith(Sink.foreach(s => println(s)))
      stream3.onComplete(_ => println("stream completed"))
      Await.ready(stream3, 10 seconds)

      val source4 = Source(stringList)
      // the solution, copied from akka-http StreamUtils.statefulMap, is to append Nil :)
      val stream4 = source4.mapConcat(_.toUpperCase() :: Nil).runWith(Sink.foreach(s => println(s)))
      stream4.onComplete(_ => println("stream completed"))
      Await.ready(stream4, 10 seconds)
    }
  }
}
