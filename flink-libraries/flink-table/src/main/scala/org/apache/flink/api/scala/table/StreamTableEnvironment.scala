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
package org.apache.flink.api.scala.table

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.{Row, TableConfig, Table}
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, DataStream}

import org.apache.flink.streaming.api.scala.asScalaStream

/**
  * The [[org.apache.flink.api.table.TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class StreamTableEnvironment(
    protected val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.api.table.StreamTableEnvironment(config) {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream, false)
    ingest(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataStream(stream, 'a, 'b)
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream, fields.toArray, false)
    ingest(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataStream]].
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream, true)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataStream[(String, Long)] = ...
    *   tableEnv.registerDataStream("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: Expression*): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream, fields.toArray, true)
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.api.table.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toDataStream[T: TypeInformation](table: Table): DataStream[T] = {
    asScalaStream(translate(table))
  }

  /**
    * Creates a [[Row]] [[JavaStream]] from an [[InputFormat]].
    *
    * @param inputFormat [[InputFormat]] from which the [[JavaStream]] is created.
    * @param typeInfo [[TypeInformation]] of the type of the [[JavaStream]].
    * @return A [[Row]] [[JavaStream]] created from the [[InputFormat]].
    */
  override private[flink] def createDataStreamSource(
      inputFormat: InputFormat[Row, _],
      typeInfo: TypeInformation[Row]): JavaStream[Row] = {

    execEnv.createInput(inputFormat)(typeInfo).javaStream
  }

}
