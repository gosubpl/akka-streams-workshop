package pl.gosub.akka_streams.workshop

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, ThrottleMode}
import org.scalatest.FreeSpec

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CustomStageSpec extends FreeSpec {
  import FutureExtensions._

  "A custom Stage should" - {
    implicit val system = ActorSystem("CustomStageSpec")
    implicit val materializer = ActorMaterializer()

    "be fun" in {
      println("Having fun at ScalaWave!")
    }
  }

}
