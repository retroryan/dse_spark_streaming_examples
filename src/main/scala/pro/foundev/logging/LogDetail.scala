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

package pro.foundev.logging

class LogDetail (
  val serviceName: String,
  val infoCount:Long ,
  val warnCount:Long ,
  val errorCount:Long){

  def +(logDetail:LogDetail): LogDetail = {
    if (serviceName != logDetail.serviceName)
      throw new RuntimeException(s"cannot add service names that do not match: current is ${serviceName} and attempted to add ${logDetail.serviceName}")
    val incomingInfo = logDetail.infoCount
    val incomingError = logDetail.errorCount
    val incomingWarn = logDetail.warnCount
    new LogDetail(
      serviceName,
      incomingInfo + infoCount,
      incomingWarn + warnCount,
      incomingError + errorCount
    )
  }
}

 object LogDetail {

   def createFromLog(log:Log): LogDetail = log.serviceName match {
     case "error" => new LogDetail(log.serviceName, 0, 0, 1)
     case "warn" => new LogDetail(log.serviceName, 0, 1, 0)
     case _ => new LogDetail(log.serviceName, 1, 0, 0)
   }
 }