name := "spark_copy_cmd"

version := "0.1"

scalaVersion := "2.12.5"
val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.spark" %% "spark-core"        % "3.0.0",
  "org.apache.spark" %% "spark-streaming"    % "3.0.0",
  "org.apache.spark" %% "spark-sql"         % "3.0.0",
  "org.postgresql" % "postgresql" % "42.2.9",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scalactic" %% "scalactic" % "3.1.0"
)

