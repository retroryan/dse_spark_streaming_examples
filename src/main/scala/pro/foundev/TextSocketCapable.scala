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

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait TextSocketCapable extends CassandraCapable {

  val hostName = "localhost"
  val port = 10034
  def connectToSocket(): (DStream[String], StreamingContext, CassandraConnector) = {
    val context = connect()
    val rdd: DStream[String] = context.streamingContext.socketTextStream(hostName,port);
    (rdd, context.streamingContext, context.connector)
  }
}
