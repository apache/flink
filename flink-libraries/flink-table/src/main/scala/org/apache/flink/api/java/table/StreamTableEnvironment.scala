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
package org.apache.flink.api.java.table

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.{Row, TableConfig, Table}
import org.apache.flink.api.table.expressions.ExpressionParser
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * The [[org.apache.flink.api.table.TableEnvironment]] for a Java [[StreamExecutionEnvironment]].
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
  * @param execEnv The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
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
    registerDataStreamInternal(name, dataStream, false)
    ingest(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> stream = ...
    *   Table tab = tableEnv.fromDataStream(stream, "a, b")
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream, exprs, false)
    ingest(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream, true)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   tableEnv.registerDataStream("myTable", set, "a, b")
    * }}}
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream, exprs, true)
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
    * @param clazz The class of the type of the resulting [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toDataStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    translate[T](table)(TypeExtractor.createTypeInfo(clazz))
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
    * @param typeInfo The [[TypeInformation]] that specifies the type of the [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toDataStream[T](table: Table, typeInfo: TypeInformation[T]): DataStream[T] = {
    translate[T](table)(typeInfo)
  }

  /**
    * Creates a [[Row]] [[DataStream]] from an [[InputFormat]].
    *
    * @param inputFormat [[InputFormat]] from which the [[DataStream]] is created.
    * @param typeInfo [[TypeInformation]] of the type of the [[DataStream]].
    * @return A [[Row]] [[DataStream]] created from the [[InputFormat]].
    */
  override private[flink] def createDataStreamSource(
      inputFormat: InputFormat[Row, _],
      typeInfo: TypeInformation[Row]): DataStream[Row] = {

    execEnv.createInput(inputFormat, typeInfo)
  }

}
