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
package org.apache.flink.table.api.java

import java.lang.{Iterable => JIterable}
import java.util.{ArrayList => JArrayList, Collection => JCollection}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.getCallLocationName
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.expressions.ExpressionParser

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.flink.table.api._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.sources.RangeInputFormat
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * The [[TableEnvironment]] for a Java [[StreamExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config  The configuration of the TableEnvironment.
  */
class BatchTableEnvironment(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.BatchTableEnvironment(execEnv, config) {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names and field nullables attributes of the [[Table]] are automatically
    * derived from the type of the
    * [[DataStream]].
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](boundedStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream, false)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](
      boundedStream: DataStream[T],
      fieldNullables: Iterable[Boolean]): Table = {
    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream, fieldNullables.toArray, false)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> stream = ...
    *   Table tab = tableEnv.fromBoundedStream(stream, "a, b")
    * }}}
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](boundedStream: DataStream[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream, exprs, false)
    scan(name)
  }


  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> stream = ...
    *   List<Boolean> fieldNullables = ...
    *   Table tab = tableEnv.fromBoundedStream(stream, fieldNullables, "a, b")
    * }}}
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](
      boundedStream: DataStream[T],
      fieldNullables: Iterable[Boolean],
      fields: String): Table = {
    val exprs = ExpressionParser
        .parseExpressionList(fields)
        .toArray

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream, exprs, fieldNullables.toArray, false)
    scan(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](name: String, boundedStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, false)
  }

  /**
    * Registers or replace the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerOrReplaceBoundedStream[T](name: String, boundedStream: DataStream[T]): Unit = {
    registerBoundedStreamInternal(name, boundedStream, true)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Iterable[Boolean]): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, fieldNullables.toArray, false)
  }


  /**
    * Registers or replace the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerOrReplaceBoundedStream[T](
                                        name: String,
                                        boundedStream: DataStream[T],
                                        fieldNullables: Iterable[Boolean]): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, fieldNullables.toArray, true)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   tableEnv.registerBoundedStream("myTable", set, "a, b")
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, exprs, false)
  }


  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   tableEnv.registerOrReplaceBoundedStream("myTable", set, "a, b")
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerOrReplaceBoundedStream[T](
                                        name: String,
                                        boundedStream: DataStream[T],
                                        fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, exprs, true)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   List<Boolean> fieldNullables = ...
    *   tableEnv.registerBoundedStream("myTable", set, fieldNullables, "a, b")
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Iterable[Boolean],
      fields: String): Unit = {
    val exprs = ExpressionParser
        .parseExpressionList(fields)
        .toArray

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, exprs, fieldNullables.toArray, false)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   List<Boolean> fieldNullables = ...
    *   tableEnv.registerOrReplaceBoundedStream("myTable", set, fieldNullables, "a, b")
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerOrReplaceBoundedStream[T](
                                        name: String,
                                        boundedStream: DataStream[T],
                                        fieldNullables: Iterable[Boolean],
                                        fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream, exprs, fieldNullables.toArray, true)
  }

  /**
    * Translates a [[Table]] into a [[DataStream]] of a specific type.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the [[DataStream]] type.
    * @tparam T The type of the [[DataStream]].
    * @return The generated [[DataStream]] operators translated by [[Table]].
    */
  def toBoundedStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    val resultType = DataTypes.extractDataType(clazz)
    TableEnvironment.validateType(resultType)
    translateToDataStream(table, resultType)
  }

  /**
    * Registers the given [[JIterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * The data passed in must not be empty, cause we need to infer the [[TypeInformation]] from it.
    *
    * @param tableName name of table.
    * @param data The [[JIterable]] to be converted.
    * @param fields field names, eg: "a, b, c"
    * @tparam T The type of the [[JIterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](
    tableName: String, data: JIterable[T], fields: String): Unit = {
    val typeInfo = TypeExtractor.createTypeInfo(data.iterator().next().getClass)
    registerCollection(tableName, data, typeInfo.asInstanceOf[TypeInformation[T]], fields)
  }

  /**
    * Registers the given [[JIterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * The data passed in must not be empty, cause we need to infer the [[TypeInformation]] from it.
    *
    * @param tableName name of table.
    * @param data The [[JIterable]] to be converted.
    * @param fieldNullables The field isNullables attributes of data.
    * @param fields field names, eg: "a, b, c"
    * @tparam T The type of the [[JIterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](tableName: String, data: JIterable[T],
    fieldNullables: Iterable[Boolean], fields: String): Unit = {

    val typeInfo = TypeExtractor.createTypeInfo(data.iterator().next().getClass)
    registerCollection(tableName, data, typeInfo.asInstanceOf[TypeInformation[T]],
      fieldNullables, fields)
  }

  private implicit def iterableToCollection[T](data: JIterable[T]): JCollection[T] = {
    val collection = new JArrayList[T]()
    data foreach (d => collection.add(d))
    collection
  }

  /**
    * Registers the given [[JIterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[JIterable]] to be converted.
    * @param typeInfo type information of [[JIterable]].
    * @param fields field names, eg: "a, b, c"
    * @tparam T The type of the [[JIterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](tableName: String, data: JIterable[T],
    typeInfo: TypeInformation[T], fields: String): Unit = {
    val boundedStream = streamEnv.createInput(new CollectionInputFormat[T](
      data,
      typeInfo.createSerializer(execEnv.getConfig)),
      typeInfo, tableName)
    boundedStream.setParallelism(1)
    if (fields == null) {
      registerBoundedStream(tableName, boundedStream)
    } else {
      registerBoundedStream(tableName, boundedStream, fields)
    }
  }

  /**
    * Registers the given [[JIterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[JIterable]] to be converted.
    * @param typeInfo type information of [[JIterable]].
    * @param fieldNullables The field isNullables attributes of data.
    * @param fields field names, eg: "a, b, c"
    * @tparam T The type of the [[JIterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](
    tableName: String,
    data: JIterable[T],
    typeInfo: TypeInformation[T],
    fieldNullables: Iterable[Boolean],
    fields: String): Unit = {
    val boundedStream = streamEnv.createInput(new CollectionInputFormat[T](
      data,
      typeInfo.createSerializer(execEnv.getConfig)),
      typeInfo, tableName)
    boundedStream.setParallelism(1)
    registerBoundedStream(tableName, boundedStream, fieldNullables, fields)
  }

  /**
    * Create a [[Table]] from sequence of elements. Typical, user can pass in a sequence of tuples,
    * the table schema type would be inferred from the tuple type: e.g.
    * {{{
    *   tEnv.fromElements((1, 2, "abc"), (3, 4, "def"))
    * }}}
    * Then the schema type would be (_1:int, _2:int, _3:varchar)
    *
    * Caution that use must pass a ''Java'' type data elements, or the inferred type
    * would be unexpected.
    *
    * @param data row data sequence
    * @tparam T row data class type
    * @return table from the data with default fields names
    */
  def fromElements[T: ClassTag : TypeInformation](data: T*): Table = {
    require(data != null, "Data must not be null.")
    fromCollection(data.asJava)
  }

  /**
    * Create a [[Table]] from a java [[JIterable]]. The table schema type would be inferred
    * from the [[JIterable]] element type.
    */
  def fromCollection[T](data: JIterable[T]): Table = {
    fromCollection(data, null)
  }

  /**
    * Create a [[Table]] from a java [[JIterable]]. Would infer table schema from the passed in
    * typeInfo.
    *
    * This passed in data must not be empty cause we need to infer [[TypeInformation]] from it.
    */
  def fromCollection[T](data: JIterable[T], fields: String): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = TypeExtractor.createTypeInfo(data.iterator().next().getClass)
    fromCollection(data, typeInfo.asInstanceOf[TypeInformation[T]], fields)
  }

  /**
    * Create a [[Table]] from a java [[JIterable]]. Would infer table schema from the passed in
    * typeInfo.
    */
  def fromCollection[T](data: JIterable[T], typeInfo: TypeInformation[T], fields: String): Table = {
    CollectionInputFormat.checkCollection(data, typeInfo.getTypeClass)
    val boundedStream = streamEnv.createInput(new CollectionInputFormat[T](
      data,
      typeInfo.createSerializer(execEnv.getConfig)),
      typeInfo, getCallLocationName())
    boundedStream.setParallelism(1)
    if (fields == null) {
      fromBoundedStream(boundedStream)
    } else {
      fromBoundedStream(boundedStream, fields)
    }
  }

  /**
    * Creates a [[Table]] with a single `DataTypes.Long` column named `id`, containing elements
    * in a range from `start` to `end` (exclusive) with step value 1.
    */
  def range(start: Long, end: Long): Table = {
    val typeInfo = new BaseRowTypeInfo(Types.LONG)
    val boundedStream = streamEnv.createInput(new RangeInputFormat(start, end),
      typeInfo, getCallLocationName())
    boundedStream.setParallelism(1)
    fromBoundedStream(boundedStream, "id")
  }
}
