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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.expressions.ExpressionParser
import org.apache.flink.api.table.{TableConfig, Table}

/**
  * The [[org.apache.flink.api.table.TableEnvironment]] for a Java batch [[DataSet]]
  * [[ExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataSet]] to a [[Table]]
  * - register a [[DataSet]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[org.apache.flink.api.table.TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataSet]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Java batch [[ExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class BatchTableEnvironment(
    protected val execEnv: ExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.api.table.BatchTableEnvironment(config) {

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T]): Table = {

    val name = createUniqueTableName()
    registerDataSetInternal(name, dataSet)
    scan(name)
  }

  /**
    * Converts the given [[DataSet]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataSet<Tuple2<String, Long>> set = ...
    *   Table tab = tableEnv.fromDataSet(set, "a, b")
    * }}}
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    val name = createUniqueTableName()
    registerDataSetInternal(name, dataSet, exprs)
    scan(name)
  }

  /**
    * Registers the given [[DataSet]] as table in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = {

    checkValidTableName(name)
    registerDataSetInternal(name, dataSet)
  }

  /**
    * Registers the given [[DataSet]] as table with specified field names in the
    * [[org.apache.flink.api.table.TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataSet<Tuple2<String, Long>> set = ...
    *   tableEnv.registerDataSet("myTable", set, "a, b")
    * }}}
    *
    * @param name The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerDataSetInternal(name, dataSet, exprs)
  }

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.api.table.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the type of the resulting [[DataSet]].
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    translate[T](table)(TypeExtractor.createTypeInfo(clazz))
  }

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.api.table.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] that specifies the type of the resulting [[DataSet]].
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = {
    translate[T](table)(typeInfo)
  }

}
