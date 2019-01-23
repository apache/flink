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
package org.apache.flink.table.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.Expression
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Holds methods to convert a [[DataStream]] into a [[Table]].
  *
  * @param dataStream The [[DataStream]] to convert.
  * @param inputType The [[TypeInformation]] for the type of the [[DataStream]].
  * @tparam T The type of the [[DataStream]].
  */
class DataStreamConversions[T](dataStream: DataStream[T], inputType: TypeInformation[T]) {

  /**
    * Converts the [[DataStream]] into a [[Table]].
    *
    * The field name of the new [[Table]] can be specified like this:
    *
    * {{{
    *   val env = StreamExecutionEnvironment.getExecutionEnvironment
    *   val tEnv = TableEnvironment.getTableEnvironment(env)
    *
    *   val stream: DataStream[(String, Int)] = ...
    *   val table = stream.toTable(tEnv, 'name, 'amount)
    * }}}
    *
    * If not explicitly specified, field names are automatically extracted from the type of
    * the [[DataStream]].
    *
    * @param tableEnv The [[StreamTableEnvironment]] in which the new [[Table]] is created.
    * @param fields The field names of the new [[Table]] (optional).
    * @return The resulting [[Table]].
    */
  @deprecated("In order to make it more explicit, use toTableFromAppendStream instead.")
  def toTable(tableEnv: StreamTableEnvironment, fields: Expression*): Table = {
    if (fields.isEmpty) {
      tableEnv.fromAppendStream(dataStream)
    } else {
      tableEnv.fromAppendStream(dataStream, fields:_*)
    }
  }

  /**
    * Converts the append [[DataStream]] into a [[Table]].
    *
    * The field name of the new [[Table]] can be specified like this:
    *
    * {{{
    *   val env = StreamExecutionEnvironment.getExecutionEnvironment
    *   val tEnv = TableEnvironment.getTableEnvironment(env)
    *
    *   val stream: DataStream[(String, Int)] = ...
    *   val table = stream.toTableFromAppendStream(tEnv, 'name, 'amount)
    * }}}
    *
    * If not explicitly specified, field names are automatically extracted from the type of
    * the [[DataStream]].
    *
    * @param tableEnv The [[StreamTableEnvironment]] in which the new [[Table]] is created.
    * @param fields The field names of the new [[Table]] (optional).
    * @return The resulting [[Table]].
    */
  def toTableFromAppendStream(tableEnv: StreamTableEnvironment, fields: Expression*): Table = {
    if (fields.isEmpty) {
      tableEnv.fromAppendStream(dataStream)
    } else {
      tableEnv.fromAppendStream(dataStream, fields:_*)
    }
  }

  /**
    * Converts the upsert [[DataStream]] with upsert messages into a [[Table]] with keys.
    *
    * The incoming messages from the source DataStream are expected to be encoded as [[Tuple2]].
    * The first field is a [[Boolean]] flag, the second field holds the record. A true [[Boolean]]
    * flag indicates an update message, a false flag indicates a delete message.
    *
    * The field name and key of the new [[Table]] can be specified like this:
    *
    * {{{
    *   val env = StreamExecutionEnvironment.getExecutionEnvironment
    *   val tEnv = TableEnvironment.getTableEnvironment(env)
    *
    *   val stream: DataStream[(Boolean, (String, Int))] = ...
    *   val table = stream.toTableFromUpsertStream(tEnv, 'name.key, 'amount)
    * }}}
    *
    * If field names are not explicitly specified, names are automatically extracted from the type
    * of the [[DataStream]].
    * If keys are not explicitly specified, an empty key will be used and the table will be a
    * single row table.
    *
    * @param tableEnv The [[StreamTableEnvironment]] in which the new [[Table]] is created.
    * @param fields The field names of the new [[Table]] (optional).
    * @return The resulting [[Table]].
    */
  def toTableFromUpsertStream(tableEnv: StreamTableEnvironment, fields: Expression*): Table = {
    if (fields.isEmpty) {
      tableEnv.fromUpsertStream(dataStream)
    } else {
      tableEnv.fromUpsertStream(dataStream, fields:_*)
    }
  }
}

