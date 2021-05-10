
scalaVersion := "2.12.10"
name := "abc-demo"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.7" % "test"

Compile / _root_.sbt.Keys.`package` := (Compile / _root_.sbt.Keys.`package` dependsOn Test / test).value

assembly / assemblyMergeStrategy := {
   case PathList("META-INF", xs@_*) => MergeStrategy.discard
   case x => MergeStrategy.first
}
