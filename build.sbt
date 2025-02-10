ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "compositional-sql-parser-with-parsley"
  )


libraryDependencies ++= Seq(
  "com.github.j-mie6" %% "parsley" % "4.5.2",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)