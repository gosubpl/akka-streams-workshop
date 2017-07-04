package pl.gosub.akka_streams.workshop

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import org.scalatest.FreeSpec

import scala.io.StdIn

class AkkaHttpFlowSpec extends FreeSpec {
  "A system using Akka Http should" - {
    implicit val system = ActorSystem("AkkaHttpFlowSpec")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    "be able to plug a Flow into Http server" in {
      // IMPORTANT! read this: http://doc.akka.io/docs/akka-http/current/scala/http/implications-of-streaming-http-entity.html
      // before trying at home!
      val httpHello = Flow[HttpRequest]
        .map { request =>
          // simple streaming "hello" response:
          HttpResponse(entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("Hello there!")))
        }

      // source for mapping incoming connections
      val serverSource = Http().bind(interface = "localhost", port = 8888)
      val streamingServer = serverSource.to(Sink.foreach { connection =>
        println("Accepted new connection from " + connection.remoteAddress)
        connection.handleWith(httpHello)
      }).run()
      println(s"Server online at http://localhost:8888/\nPress RETURN to stop...")
      StdIn.readLine() // let it run until user presses return
      streamingServer
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate()) // and shutdown when done
    }
  }
}
