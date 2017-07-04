name := "akka-streams-workshop"

version := "0.0.1"

scalaVersion := "2.12.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.1" % "test"

libraryDependencies ++= Seq(
  // akka-streams dependencies
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.3" % Test,

  // alpakka dependencies (vary per connector)
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.10",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.10",

  // akka-http dependencies
  "com.typesafe.akka" %% "akka-http" % "10.0.9",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.9" % Test
)
