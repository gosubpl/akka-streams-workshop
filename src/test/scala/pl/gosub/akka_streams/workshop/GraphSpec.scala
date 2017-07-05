package pl.gosub.akka_streams.workshop

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, ThrottleMode}
import org.scalatest.FreeSpec

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// for graphs -> one example just flow the other with integrated source
// which will need this more complex builder
// Source, Flow, etc BidiFlow.fromGraph(...)
class GraphSpec extends FreeSpec {

}
