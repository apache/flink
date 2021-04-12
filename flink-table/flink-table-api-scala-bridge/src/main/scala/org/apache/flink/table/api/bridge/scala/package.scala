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
package org.apache.flink.table.api.bridge

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.internal.TableImpl
import org.apache.flink.table.api.{ImplicitExpressionConversions, ImplicitExpressionOperations, Table, ValidationException}
import org.apache.flink.types.Row

import _root_.scala.language.implicitConversions

/**
  * == Table & SQL API with Flink's DataStream API ==
  *
  * This package contains the API of the Table & SQL API that bridges to Flink's [[DataStream]] API
  * for the Scala programming language. Users can create [[Table]]s from [[DataStream]]s on which
  * relational operations can be performed. Tables can also be converted back to [[DataStream]]s for
  * further processing.
  *
  * For accessing all API classes and implicit conversions, use the following imports:
  *
  * {{{
  *   import org.apache.flink.table.api._
  *   import org.apache.flink.table.api.bridge.scala._
  * }}}
  *
  * More information about the entry points of the API can be found in [[StreamTableEnvironment]].
  *
  * Available implicit expressions are listed in [[ImplicitExpressionConversions]] and
  * [[ImplicitExpressionOperations]].
  *
  * Available implicit table-to-stream conversions are listed in this package object.
  *
  * Please refer to the website documentation about how to construct and run table programs that are
  * connected to the DataStream API.
  */
package object scala {

  implicit def tableConversions(table: Table): TableConversions = {
    new TableConversions(table.asInstanceOf[TableImpl])
  }

  implicit def dataSetConversions[T](set: DataSet[T]): DataSetConversions[T] = {
    new DataSetConversions[T](set, set.getType())
  }

  implicit def dataStreamConversions[T](set: DataStream[T]): DataStreamConversions[T] = {
    new DataStreamConversions[T](set, set.dataType)
  }

  implicit def table2RowDataSet(table: Table): DataSet[Row] = {
    val tableEnv = table.asInstanceOf[TableImpl].getTableEnvironment
    if (!tableEnv.isInstanceOf[BatchTableEnvironment]) {
      throw new ValidationException("Table cannot be converted into a DataSet. " +
        "It is not part of a batch table environment.")
    }
    tableEnv.asInstanceOf[BatchTableEnvironment].toDataSet[Row](table)
  }

  implicit def table2RowDataStream(table: Table): DataStream[Row] = {
    val tableEnv = table.asInstanceOf[TableImpl].getTableEnvironment
    if (!tableEnv.isInstanceOf[StreamTableEnvironment]) {
      throw new ValidationException("Table cannot be converted into a DataStream. " +
        "It is not part of a stream table environment.")
    }
    tableEnv.asInstanceOf[StreamTableEnvironment].toAppendStream[Row](table)
  }
}
