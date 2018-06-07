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

import java.util.Objects

/**
  * Temperature event.
  */
class TemperatureEvent(override val rackID: Int, val temperature: Double)
    extends MonitoringEvent(rackID) {

  override def equals(o: Any): Boolean = o match {
    case other: TemperatureEvent =>
      super.equals(other) && java.lang.Double.compare(other.temperature, temperature) == 0
    case _ => false
  }

  override def hashCode: Int = Objects.hash(
    super.hashCode.asInstanceOf[Integer], temperature.asInstanceOf[java.lang.Double])

  override def toString: String = "TemperatureEvent(" + rackID + ", " + temperature + ")"

}
