
# Spark_Cassandra_Streaming Example

Extract web server logs and send them to Cassandra with Spark-Kafka Streaming


## Tech Stack

* filebeat
* Kafka
* Spark Structured Streaming
* Cassandra

## Installation

Filebeat is a light weight agent on server for log data shipping, which can monitors log files, log directories changes and forward log lines to different target Systems like Logstash, Kafka ,elasticsearch or files etc.

Install filebeat 7.x on CentOS 7

Step 1: First We need to download the filebeat rpm using the below command.

```bash
curl -L -O https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-7.10.2-x86_64.rpm
```

Step 2: Install the above downloaded package on the system,

```bash
sudo rpm -vi filebeat-7.10.2-x86_64.rpm
```

Step 3: Then go to /etc/filebeat folder and open the filebeat.yml file ,remove the exising configuration and paste the below configuration.

```bash
filebeat.inputs:
- type: log 
  enabled: true 
  paths: 
     - /var/log/serverlog/access.log

output.kafka:
  hosts: ["node2.example.com:9092,node3.example.com:9092,node4.example.com:9092"]
  topic: "serverlog"
  codec.format:
    string: '%{[@timestamp]} %{[message]}'
  partition.round_robin:
    reachable_only: false
  required_acks: 1
  max_message_bytes: 1000000
  close_inactive: 10m
```

Step 4: Start and enable filebeat service on boot

```bash
sudo systemctl enable filebeat
```

Confirm that the service is running.

```bash
sudo systemctl status filebeat
```

To check the version of filebeat installed on the system, Run the below command.

```bash
$ filebeat version
filebeat version 7.10.2 (amd64), libbeat 7.10.2 [aacf9ecd9c494aa0908f61fbca82c906b16562a8 built 2021-01-12 23:11:24 +0000 UTC]
```
## Usage/Examples

1) Create a Kafka topic.

```bash
kafka-topics --bootstrap-server [HOST1:PORT1] --create --topic [TOPIC] --partitions <no_of_partitions> --replication-factor <replication_factor>
```

2) Modify the filebeat config file to hit the server log file every second and send the data to kafka topic.

```javascript
$ cat /etc/filebeat/filebeat.yml
filebeat.inputs:
- type: log 
  enabled: true 
  paths: 
     - /var/log/serverlog/access.log

output.kafka:
  hosts: ["node2.example.com:9092,node3.example.com:9092,node4.example.com:9092"]
  topic: "serverlog"
  codec.format:
    string: '%{[@timestamp]} %{[message]}'
  partition.round_robin:
    reachable_only: false
  required_acks: 1
  max_message_bytes: 1000000
  close_inactive: 10m
```

3) Create a Spark Streaming Code file to read data from Kafka topic and write the required data to Cassandra.

```javascript
$ cat SparkStreamingToCassandra.scala

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

```

4) Prepare a pom.xml file for your project

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>ServerLogAnalysis</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.4.7</spark.version>
        <jackson.version>2.11.0</jackson.version>
        <spark.scope>provided</spark.scope>
    </properties>

    <!-- Developers -->
    <developers>
        <developer>
            <id>deepakpanda93</id>
            <name>Deepak Panda</name>
            <email>deepakpanda93@gmail.com</email>
            <url>https://github.com/deepakpanda93</url>
        </developer>
    </developers>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>com.datastax.spark</groupId>
            <artifactId>spark-cassandra-connector_2.11</artifactId>
            <version>2.4.3</version>
        </dependency>

    </dependencies>
</project>
```



## Run Locally

Start the filebeat

```bash
sudo systemctl start filebeat
```

Start the spark-kafka streaming

```bash
I used InteliJ to run the project. But one can build the project, deploy the JAR on the cluster and execute using spark-submit
```




## Screenshots

### Sample Server logs

![App Screenshot](https://github.com/deepakpanda93/ServerLog_Streaming_Cassandra/blob/master/src/main/assets/SampleAccessLogData.png?raw=true)

### Server logs Ingested into Kafka Topic (Kafka Console Consumer)

![App Screenshot](https://github.com/deepakpanda93/ServerLog_Streaming_Cassandra/blob/master/src/main/assets/Kafka_logs_data.png?raw=true)


### Spark Streaming Ingested Data

![App Screenshot](https://github.com/deepakpanda93/ServerLog_Streaming_Cassandra/blob/master/src/main/assets/SparkStreamingCassandra_printSchema.png?raw=true)
![App Screenshot](https://github.com/deepakpanda93/ServerLog_Streaming_Cassandra/blob/master/src/main/assets/SparkCassandraStreamingData_batch.png?raw=true)


### Final output in Cassandra Table

![App Screenshot](https://github.com/deepakpanda93/ServerLog_Streaming_Cassandra/blob/master/src/main/assets/CassandraTableData.png?raw=true)

## 🚀 About Me

## Hi, I'm Deepak! 👋

I'm a Big Data Engineer...




## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://github.com/deepakpanda93)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/deepakpanda93)

