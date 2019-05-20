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

package org.apache.flink.table.python

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.java.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.typeutils.PythonTypeUtil
import org.apache.flink.types.Row

object PythonTableUtil {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The schema of the [[Table]] is derived from the specified schemaString.
    *
    * @param tableEnv The table environment.
    * @param dataStream The [[DataStream]] to be converted.
    * @param dataType The type information of the table.
    * @return The converted [[Table]].
    */
  def fromDataStream(
      tableEnv: StreamTableEnvironment,
      dataStream: DataStream[Array[Object]],
      dataType: TypeInformation[Row]): Table = {
    val convertedDataStream = dataStream.map(
      new MapFunction[Array[Object], Row] {
        override def map(value: Array[Object]): Row =
          PythonTypeUtil.convertTo(dataType).apply(value).asInstanceOf[Row]
      }).returns(dataType.asInstanceOf[TypeInformation[Row]])

    tableEnv.fromDataStream(convertedDataStream)
  }

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The schema of the [[Table]] is derived from the specified schemaString.
    *
    * @param tableEnv The table environment.
    * @param dataSet The [[DataSet]] to be converted.
    * @param dataType The type information of the table.
    * @return The converted [[Table]].
    */
  def fromDataSet(
      tableEnv: BatchTableEnvironment,
      dataSet: DataSet[Array[Object]],
      dataType: TypeInformation[Row]): Table = {
    val convertedDataSet = dataSet.map(
      new MapFunction[Array[Object], Row] {
        override def map(value: Array[Object]): Row =
          PythonTypeUtil.convertTo(dataType).apply(value).asInstanceOf[Row]
      }).returns(dataType.asInstanceOf[TypeInformation[Row]])

    tableEnv.fromDataSet(convertedDataSet)
  }
}
