name := "practica"

version := "0.1"

scalaVersion := "2.11.12"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8
//En Maven se llama Spark Integration For Kafka 0.8 La 0.8 solo vale para scala 2.10 y 2.11
//La 0.10 vale para 2.11 y 2.12
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
//De esta no existe la version 0-8
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4"
