package pl.gosub.akka_streams.workshop

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest.FreeSpec

class HelloSpec extends FreeSpec {
  "A friendly greeter" - {
    implicit val system = ActorSystem("Examples")
    implicit val materializer = ActorMaterializer()
    "should say hello world!" in {
      val source: Source[Int, NotUsed] = Source(1 to 100)
      source.runForeach(i => println(i))(materializer)
      assert("hello world" == "hello world")
    }
  }
}
