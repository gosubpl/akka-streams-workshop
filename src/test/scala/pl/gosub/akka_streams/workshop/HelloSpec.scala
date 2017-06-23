package pl.gosub.akka_streams.workshop

import org.scalatest.FreeSpec

class HelloSpec extends FreeSpec {
  "A friendly greeter" - {
    "should say hello world!" in {
      assert("hello world" == "hello world")
    }
  }
}
