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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.api.table.typeutils.IntervalTypeInfo

/**
  * This class enumerates all supported types of the Table API.
  */
object Types {

  val STRING = BasicTypeInfo.STRING_TYPE_INFO
  val BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO

  val BYTE = BasicTypeInfo.BYTE_TYPE_INFO
  val SHORT = BasicTypeInfo.SHORT_TYPE_INFO
  val INT = BasicTypeInfo.INT_TYPE_INFO
  val LONG = BasicTypeInfo.LONG_TYPE_INFO
  val FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO
  val DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO
  val DECIMAL = BasicTypeInfo.BIG_DEC_TYPE_INFO

  val DATE = SqlTimeTypeInfo.DATE
  val TIME = SqlTimeTypeInfo.TIME
  val TIMESTAMP = SqlTimeTypeInfo.TIMESTAMP
  val INTERVAL_MONTHS = IntervalTypeInfo.INTERVAL_MONTHS
  val INTERVAL_MILLIS = IntervalTypeInfo.INTERVAL_MILLIS

}
