package pro.foundev

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

import _root_.java.text.SimpleDateFormat

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext._

object WindowedCalculationsAndEventTriggering extends TextSocketCapable{

  def main(args: Array[String]): Unit ={

    val (rdd, sparkContext, connector) = connectToSocket()
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val transactionList = rdd.map(line => {
     val columns = line.split(",")
      val taxId = columns(0)
      val name = columns(1)
      val merchant = columns(2)
      val amount = BigDecimal(columns(3))
      val transactionDate = format.parse(columns(4))
      println(line)
      (taxId,(name, merchant, amount, transactionDate))
    }).cache()
    val warningsTableName = "warnings"
    connector.withSessionDo(session=>session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${warningsTableName}"))
    connector.withSessionDo(session=>session.execute("CREATE TABLE IF NOT EXISTS "+
      s"${keySpaceName}.${warningsTableName} (ssn text, id timeuuid, amount decimal, rule text, PRIMARY KEY(ssn, id))"))
    //setup warning on more than certain number of transactions by user in a 60 second window, every 10 seconds
    transactionList.window(Seconds(60), Seconds(10))
     .map(record=>(record._1, record._2._3))
     .reduceByKey(_+_)
    .filter(_._2>BigDecimal(9999))
    .foreachRDD(rdd=>
      rdd.foreach(row=>{
        println(s"Warning about user with taxId ${row._1} they've submitted ${row._2} in the past 60 seconds")
        connector.withSessionDo(session=>{
          val prepared = session.prepare(s"INSERT INTO ${keySpaceName}.${warningsTableName} (ssn, id, amount, rule)"+
            "values (?,now(),?,'OVER_DOLLAR_AMOUNT')")
          session.execute(prepared.bind(row._1,row._2.bigDecimal ))
        })
      }

    ))
    sparkContext.start()
    sparkContext.awaitTermination()
  }
}
