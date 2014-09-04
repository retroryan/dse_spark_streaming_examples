

name := "dse_spark_streaming_examples"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "0.9.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "0.9.1" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-rc5" withSources() withJavadoc()

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.0.0-rc5" withSources() withJavadoc()

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(("com.typesafe.play" %% "play-json" % "2.2.1"))

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings