package pl.gosub.akka_streams.workshop

import java.nio.file.FileSystems

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.file.scaladsl
import akka.stream.scaladsl.Source
import akka.stream.ActorMaterializer
import akka.util.ByteString
import org.scalatest.FreeSpec

import scala.concurrent.duration._

class AlpakkaSourceSpec extends FreeSpec {
  import FutureExtensions._
  "A system using Alpakka should" - {
    implicit val system = ActorSystem("AlpakkaSourceSpec")
    implicit val materializer = ActorMaterializer()

    "be able to monitor and parse a file" in {
      val fsPath = this.getClass.getResource("/multiline_text.txt").getPath
      println("echo \"what;makes;you;think;so?\" >> " + fsPath)
      val fs = FileSystems.getDefault
//      val lins = scala.io.Source.fromFile(fs.getPath(fsPath).toString).getLines.toList
//      println(lins)

      // be careful to check your assumptions, Alpakka FileTailSource needs at least one end-line
      // to push something into the stream
      // See the source of .lines to understand (hint: .via(Framing.delimiter("\n")) )
      val lines: Source[String, NotUsed] = scaladsl.FileTailSource.lines(
        path = fs.getPath(fsPath),
        maxLineSize = 8192,
        pollingInterval = 500.millis
      )
      val stream = lines.mapConcat(line => line.split(";").toList).runForeach(println)
      stream.awaitVeryLongAndPrint
    }

    "be able to read and parse a CSV file" in {
      val fsPath = this.getClass.getResource("/test.csv").getPath
      val fs = FileSystems.getDefault
      // here we are reading raw ByteString stream
      val lines: Source[ByteString, NotUsed] = scaladsl.FileTailSource.apply(
        path = fs.getPath(fsPath),
        startingPosition = 0,
        maxChunkSize = 8192,
        pollingInterval = 500.millis
      )
      val stream = lines.via(CsvParsing.lineScanner())
        .map(list => list.map(_.utf8String))
        .runForeach(println)
      stream.awaitVeryLongAndPrint
    }
  }
}
