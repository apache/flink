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
package org.apache.flink.table.api.bridge.scala

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{Schema, Table, TableEnvironment, ValidationException}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.expressions.Expression
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

/**
 * Holds methods to convert a Scala [[DataStream]] into a [[Table]].
 *
 * @param dataStream
 *   The [[DataStream]] to convert.
 * @tparam T
 *   The external type of the [[DataStream]].
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@PublicEvolving
class DataStreamConversions[T](dataStream: DataStream[T]) {

  /**
   * Converts the given [[DataStream]] into a [[Table]].
   *
   * See [[StreamTableEnvironment.fromDataStream(DataStream)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @return
   *   The converted [[Table]].
   */
  def toTable(tableEnv: StreamTableEnvironment): Table = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    tableEnv.fromDataStream(dataStream)
  }

  /**
   * Converts the given [[DataStream]] into a [[Table]].
   *
   * See [[StreamTableEnvironment.fromDataStream(DataStream, Schema)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param schema
   *   The customized schema for the final table.
   * @return
   *   The converted [[Table]].
   */
  def toTable(tableEnv: StreamTableEnvironment, schema: Schema): Table = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    tableEnv.fromDataStream(dataStream, schema)
  }

  /**
   * Creates a view from the given [[DataStream]] in a given path. Registered tables can be
   * referenced in SQL queries.
   *
   * See [[StreamTableEnvironment.createTemporaryView(String, DataStream)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param path
   *   The path under which the [[DataStream]] is created. See also the [[TableEnvironment]] class
   *   description for the format of the path.
   */
  def createTemporaryView(tableEnv: StreamTableEnvironment, path: String): Unit = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    tableEnv.createTemporaryView(path, dataStream)
  }

  /**
   * Creates a view from the given [[DataStream]] in a given path. Registered tables can be
   * referenced in SQL queries.
   *
   * See [[StreamTableEnvironment.createTemporaryView(String, DataStream, Schema)]] for more
   * information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param path
   *   The path under which the [[DataStream]] is created. See also the [[TableEnvironment]] class
   *   description for the format of the path.
   */
  def createTemporaryView(tableEnv: StreamTableEnvironment, path: String, schema: Schema): Unit = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    tableEnv.createTemporaryView(path, dataStream, schema)
  }

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * See [[StreamTableEnvironment.fromChangelogStream(DataStream)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @return
   *   The converted [[Table]].
   */
  def toChangelogTable(tableEnv: StreamTableEnvironment): Table = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    if (dataStream.dataType.getTypeClass != classOf[Row]) {
      throw new ValidationException(
        "DataStream cannot be converted to a Table. It must contain instances of Row.")
    }
    tableEnv.fromChangelogStream(dataStream.asInstanceOf[DataStream[Row]])
  }

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * See [[StreamTableEnvironment.fromChangelogStream(DataStream, Schema)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param schema
   *   The customized schema for the final table.
   * @return
   *   The converted [[Table]].
   */
  def toChangelogTable(tableEnv: StreamTableEnvironment, schema: Schema): Table = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    if (dataStream.dataType.getTypeClass != classOf[Row]) {
      throw new ValidationException(
        "DataStream cannot be converted to a Table. It must contain instances of Row.")
    }
    tableEnv.fromChangelogStream(dataStream.asInstanceOf[DataStream[Row]], schema)
  }

  /**
   * Converts the given [[DataStream]] of changelog entries into a [[Table]].
   *
   * See [[StreamTableEnvironment.fromChangelogStream(DataStream, Schema)]] for more information.
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param schema
   *   The customized schema for the final table.
   * @param changelogMode
   *   The expected kinds of changes in the incoming changelog.
   * @return
   *   The converted [[Table]].
   */
  def toChangelogTable(
      tableEnv: StreamTableEnvironment,
      schema: Schema,
      changelogMode: ChangelogMode): Table = {
    Preconditions.checkNotNull(tableEnv, "Table environment must not be null.")
    if (dataStream.dataType.getTypeClass != classOf[Row]) {
      throw new ValidationException(
        "DataStream cannot be converted to a Table. It must contain instances of Row.")
    }
    tableEnv.fromChangelogStream(dataStream.asInstanceOf[DataStream[Row]], schema, changelogMode)
  }

  // ----------------------------------------------------------------------------------------------
  // Legacy before FLIP-136
  // ----------------------------------------------------------------------------------------------

  /**
   * Converts the [[DataStream]] into a [[Table]].
   *
   * The field names of the new [[Table]] can be specified like this:
   *
   * {{{
   *   val env = StreamExecutionEnvironment.getExecutionEnvironment
   *   val tEnv = StreamTableEnvironment.create(env)
   *
   *   val stream: DataStream[(String, Int)] = ...
   *   val table = stream.toTable(tEnv, 'name, 'amount)
   * }}}
   *
   * If not explicitly specified, field names are automatically extracted from the type of the
   * [[DataStream]].
   *
   * @param tableEnv
   *   The [[StreamTableEnvironment]] in which the new [[Table]] is created.
   * @param fields
   *   The field names of the new [[Table]] (optional).
   * @return
   *   The resulting [[Table]].
   */
  def toTable(tableEnv: StreamTableEnvironment, fields: Expression*): Table = {
    if (fields.isEmpty) {
      toTable(tableEnv)
    } else {
      tableEnv.fromDataStream(dataStream, fields: _*)
    }
  }
}
