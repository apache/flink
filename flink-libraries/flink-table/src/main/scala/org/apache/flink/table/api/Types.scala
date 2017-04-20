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

import org.apache.flink.api.common.typeinfo.{Types, TypeInformation}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.api.common.typeinfo.{Types => JTypes}

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

  val SQL_DATE = JTypes.SQL_DATE
  val SQL_TIME = JTypes.SQL_TIME
  val SQL_TIMESTAMP = JTypes.SQL_TIMESTAMP
  val INTERVAL_MONTHS = TimeIntervalTypeInfo.INTERVAL_MONTHS
  val INTERVAL_MILLIS = TimeIntervalTypeInfo.INTERVAL_MILLIS

  /**
    * Generates RowTypeInfo with default names (f1, f2 ..).
    * same as ``new RowTypeInfo(types)``
    *
    * @param types of Row fields. e.g. ROW(Types.STRING, Types.INT)
    */
  def ROW[T: Manifest](types: TypeInformation[_]*) = {
    JTypes.ROW(types: _*)
  }

  /**
    * Generates RowTypeInfo.
    * same as ``new RowTypeInfo(types, names)``
    *
    * @param fields of Row. e.g. ROW(("name", Types.STRING), ("number", Types.INT))
    */
  def ROW_NAMED(fields: (String, TypeInformation[_])*) = {
    val names = fields.toList.map(_._1).toArray
    val types = fields.toList.map(_._2)
    JTypes.ROW_NAMED(names, types: _*)
  }
}
