
/*
 * Copyright 2014 Ryan Svihla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pro.foundev

import com.datastax.spark.connector.streaming._
import org.apache.spark._
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.driver.core.ConsistencyLevel
import pro.foundev.logging.Log
import pro.foundev.logging.LogDetail

object CounterReplacementOnIngest extends CassandraCapable{


  def main(args: Array[String]): Unit = {
    this.withAuth = true;
    val context = connect()
    val topicMap = Map("logs" -> 1)
    val connector = context.connector
    val logs = "logs";
    val serviceNameDetails = "service_name_details";
    connector.withSessionDo(session=>session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${logs}"))
    connector.withSessionDo(session=>session.execute("CREATE TABLE IF NOT EXISTS "+
      s"${keySpaceName}.${logs} (id text, service_name text, type text, message text, primary key(id))"))
    connector.withSessionDo(session=>session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${serviceNameDetails}"))
     connector.withSessionDo(session=>session.execute("CREATE TABLE IF NOT EXISTS "+
      s"${keySpaceName}.${serviceNameDetails} (service_name text, error_count bigint, warn_count bigint, info_count bigint, primary key(service_name))"))

    val kafkaStream = KafkaUtils.createStream(context.streamingContext, "localhost", "consumer-group", topicMap);
    kafkaStream.map(
        message => {
          val log = new Log(message)
          connector.withSessionDo(session => {
            val preparedStatement = session.prepare(s"INSERT INTO ${keySpaceName}.${logs} " +
              s"(id, service_name, type, message) values (?,?,?,?,?)")
            preparedStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            session.execute(preparedStatement.bind(log.messageId,
              log.serviceName, log.eventType, log.messageBody))
            (log.serviceName, LogDetail.createFromLog(log))
          })
        })
    .reduceByKey(_+_)
    .foreachRDD(rdd=>rdd.foreach(row=>{
      connector.withSessionDo(session=>{
        val preparedStatement = session.prepare(s"select * from ${keySpaceName}.${logs} where service_name = ?}")
        preparedStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
        val serviceName = row._1
        val rowToUpdate = session.execute(preparedStatement.bind(serviceName)).one()
        val errorCount = rowToUpdate.getLong("error_count")
        val warnCount = rowToUpdate.getLong("warn_count")
        val infoCount = rowToUpdate.getLong("info_count")
        val newLogDetail = new LogDetail(serviceName,infoCount,warnCount, errorCount) + row._2
        val safeUpdate = (param:String, updated: Long, previous: Long) => {
          session.execute(s"UPDATE ${keySpaceName}.${logs} set ${param}_count = ? WHERE service_name = ? IF ${param}_count = ?"
            ,updated: java.lang.Long , newLogDetail.serviceName, previous: java.lang.Long
          )
          //FIXME retry with new value logic
        }
        safeUpdate("error", newLogDetail.errorCount, errorCount)
        safeUpdate("warn", newLogDetail.warnCount, warnCount)
        safeUpdate("info", newLogDetail.infoCount, infoCount)
      })
    }))
    context.streamingContext.start()
    context.streamingContext.awaitTermination()
  }

}
