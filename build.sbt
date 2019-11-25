name := "hw3"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in(Compile, run) := Some("com.ashessin.cs441.hw3.stocksim.Start")
mainClass in(Compile, packageBin) := Some("com.ashessin.cs441.hw3.stocksim.Start")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false)

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "com.google.code.findbugs" % "jsr305" % "3.0.2",

  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
  "org.apache.spark" %% "spark-mllib" % "2.4.4",

  // https://mvnrepository.com/artifact/org.apache.commons/commons-math3
  "org.apache.commons" % "commons-math3" % "3.6.1",

  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % "1.4.0",


  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"

)