/**
 * Copyright 2014 Ryan Svihla

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

trait CassandraCapable {

  val keySpaceName =  "tester"
  val fullTableName =  "streaming_demo"

  def connect(): CassandraContext = {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("spark://127.0.0.1:7077")
      .setAppName("Windowed_Rapid_Transaction_Check")
      .setJars(Array("target/scala-2.10/dse_spark_streaming_examples-assembly-0.1.jar"))
    val connector = CassandraConnector(conf)
    connector.withSessionDo(session => {
      session.execute(s"create keyspace if not exists ${keySpaceName} with replication = { 'class':'SimpleStrategy', " +
        "'replication_factor':1}")
      session.execute(s"create table if not exists ${keySpaceName}.${fullTableName} " +
        "(marketId text, id int, version int, value text, PRIMARY KEY((id), version))")
    })

    val ssc = new StreamingContext(conf, Milliseconds(500))
    new CassandraContext(connector, ssc.cassandraTable(keySpaceName, fullTableName),
    ssc)
  }
}
