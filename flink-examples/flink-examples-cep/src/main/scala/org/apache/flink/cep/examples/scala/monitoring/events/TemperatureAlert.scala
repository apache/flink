/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.examples.scala.monitoring.events

import java.time.LocalDateTime
import java.util.Objects

/**
  * Temperature alert event.
  *
  * @param warningTime the time when last warning occurred.
  * @param datetime the time when this alert is generated.
  */
class TemperatureAlert(val rackID: Int,
    val warningTime: LocalDateTime,
    val datetime: LocalDateTime = LocalDateTime.now()) {

  override def equals(o: Any): Boolean = o match {
    case other: TemperatureAlert =>
      other.rackID == rackID &&
        other.warningTime.equals(warningTime) &&
        other.datetime.equals(datetime)
    case _ => false
  }

  override def hashCode: Int = Objects.hash(rackID.asInstanceOf[Integer], warningTime, datetime)

  override def toString: String =
    s"TemperatureAlert($rackID, warningTime=$warningTime, alertTime=$datetime)"

}
