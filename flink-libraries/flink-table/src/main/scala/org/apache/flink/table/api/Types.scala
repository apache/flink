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
package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.api.java.typeutils.{Types => JTypes}

/**
  * This class enumerates all supported types of the Table API.
  */
object Types extends JTypes {

  val STRING = JTypes.STRING
  val BOOLEAN = JTypes.BOOLEAN

  val BYTE = JTypes.BYTE
  val SHORT = JTypes.SHORT
  val INT = JTypes.INT
  val LONG = JTypes.LONG
  val FLOAT = JTypes.FLOAT
  val DOUBLE = JTypes.DOUBLE
  val DECIMAL = JTypes.DECIMAL

  val DATE = JTypes.DATE
  val TIME = JTypes.TIME
  val TIMESTAMP = JTypes.TIMESTAMP
  val INTERVAL_MONTHS = TimeIntervalTypeInfo.INTERVAL_MONTHS
  val INTERVAL_MILLIS = TimeIntervalTypeInfo.INTERVAL_MILLIS

  def ROW(types: TypeInformation[_]*) = JTypes.ROW(types: _*)

  def ROW(fieldNames: Array[String], types: TypeInformation[_]*) =
    JTypes.ROW(fieldNames, types: _*)
}
