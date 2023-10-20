ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "CQ_compiler_last_attempt",
    libraryDependencies ++= Seq(
      "org.apache.arrow" % "arrow-vector" % "13.0.0",
      "org.apache.arrow" % "arrow-memory" % "13.0.0",
      "org.apache.arrow" % "arrow-format" % "13.0.0",
      "org.apache.arrow" % "arrow-dataset" % "13.0.0",
      "org.apache.arrow" % "arrow-memory-netty" % "13.0.0",
      "org.immutables" % "value" % "2.9.2"
    )
  )
