package com.deepak.spark.cassandra

import org.apache.spark.sql.{Dataset, SparkSession}

case class ServerLog(host: String, timestamp: String, path: String, statuscode: Int, content_bytes: Long)

object SparkStreamingToCassandra {

  val cassandraKeyspace = "sparkdb"
  val CassandraTable = "weblog"

  private def sendtoCassandra(batchDF : Dataset[ServerLog], batchId : Long): Unit = {
    batchDF.show()
    batchDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> CassandraTable, "keyspace" -> cassandraKeyspace)).save()
  }

  def main(args: Array[String]): Unit = {

    val appName = "Spark Cassandra Integration"

    // Creating the SparkSession object
    val spark: SparkSession = SparkSession.builder().master("local").appName(appName).getOrCreate()
    import spark.implicits._

    val kafkaBootstrapServers = "node2.example.com:9092,node3.example.com:9092,node4.example.com:9092" //args(0)
    val inputTopicNames = "serverlog" //args(1)

    val inputDf = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", kafkaBootstrapServers).
      option("subscribe", inputTopicNames).
      option("startingOffsets", "latest").
      option("kafka.security.protocol","PLAINTEXT").
      load().selectExpr("CAST(value AS STRING)").as[String]

    inputDf.printSchema()

    val filterDF = inputDf.map(x => {
      val columns = x.toString().split(" ")
      ServerLog(columns(1), columns(4).replace("[",""), columns(7), columns(9).toInt, columns(10).toLong)
    })

    filterDF.printSchema()

    val outputDF = filterDF.writeStream.foreachBatch((batchDF: Dataset[ServerLog], batchId : Long) => sendtoCassandra(batchDF, batchId))
      .outputMode("append")

    outputDF.start().awaitTermination()

  }

}
