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

import _root_.java.util.logging.LogManager

import org.apache.spark.streaming.StreamingContext._
import org.slf4j.Logger

object JoiningDstreams extends TextSocketCapable{


  def main(args: Array[String]): Unit ={
    val logManager = LogManager.getLogManager()
    val logger = logManager.getLogger("joining")
    val (socketRdd, sparkContext, connector) = connectToSocket()

    val tweetsAuthors = socketRdd.map(tweet => {
      val columns = tweet.split(",")
      val tweetId = columns(0).toInt
      val author = columns(1)
      (tweetId, author)
    })
    tweetsAuthors.count()


    val tweetTimeline = "tweets"
    connector.withSessionDo(session=>session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${tweetTimeline}"))
    connector.withSessionDo(session=>session.execute("CREATE TABLE IF NOT EXISTS "+
      s"${keySpaceName}.${tweetTimeline} (tweetId int, author text, " +
      s"text text, PRIMARY KEY(tweetId))"))

    val tweetBody = sparkContext.socketTextStream(hostName, 10040)
    .map(rows=> {
      val columns =rows.split(",")
      val tweetId = columns(0).toInt
      val text = columns(1)
      (tweetId, text)
      }
    )
    tweetBody.join(tweetsAuthors)
    .foreachRDD(rdd=>rdd.foreach(tweet=>
      connector.withSessionDo(session=>{
        val tweetId: Integer = tweet._1
        val author = tweet._2._2
        val text = tweet._2._1
        val statement = session.prepare(s"INSERT INTO ${keySpaceName}.${tweetTimeline} (tweetId, author, text) " +
          s"values (?,?,?)")
        session.execute(statement.bind(tweetId, author,text))
    })))

    sparkContext.start()
    sparkContext.awaitTermination()
  }
}
