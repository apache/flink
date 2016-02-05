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
package org.apache.flink.api.table

import java.util.TimeZone

/**
 * A config to define the runtime behavior of the Table API.
 */
class TableConfig extends Serializable {

  /**
   * Defines the timezone for date/time/timestamp conversions.
   */
  private var timeZone: TimeZone = TimeZone.getTimeZone("UTC")

  /**
   * Defines if all fields need to be checked for NULL first.
   */
  private var nullCheck: Boolean = false

  /**
    * Defines if efficient types (such as Tuple types or Atomic types)
    * should be used within operators where possible.
    */
  private var efficientTypeUsage = false

  /**
   * Sets the timezone for date/time/timestamp conversions.
   */
  def setTimeZone(timeZone: TimeZone): Unit = {
    require(timeZone != null, "timeZone must not be null.")
    this.timeZone = timeZone
  }

  /**
   * Returns the timezone for date/time/timestamp conversions.
   */
  def getTimeZone = timeZone

  /**
   * Returns the NULL check. If enabled, all fields need to be checked for NULL first.
   */
  def getNullCheck = nullCheck

  /**
   * Sets the NULL check. If enabled, all fields need to be checked for NULL first.
   */
  def setNullCheck(nullCheck: Boolean): Unit = {
    this.nullCheck = nullCheck
  }

  /**
    * Returns the usage of efficient types. If enabled, efficient types (such as Tuple types
    * or Atomic types) are used within operators where possible.
    *
    * NOTE: Currently, this is an experimental feature.
    */
  def getEfficientTypeUsage = efficientTypeUsage

  /**
    * Sets the usage of efficient types. If enabled, efficient types (such as Tuple types
    * or Atomic types) are used within operators where possible.
    *
    * NOTE: Currently, this is an experimental feature.
    */
  def setEfficientTypeUsage(efficientTypeUsage: Boolean): Unit = {
    this.efficientTypeUsage = efficientTypeUsage
  }

}

object TableConfig {
  def DEFAULT = new TableConfig()
}
