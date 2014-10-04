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

import play.api.libs.json._
import play.api.libs.functional.syntax._


class Log(val message: (String, String)) {

  val messageId:String = message._1.toString
  val messageBody: JsValue = Json.parse(message._2.toString)
  val serviceName = (messageBody \ ("serviceName")).as[String]
  val eventType = (messageBody \ ("type")).as[String]
  val messageText = (messageBody \("text")).as[String]

}
