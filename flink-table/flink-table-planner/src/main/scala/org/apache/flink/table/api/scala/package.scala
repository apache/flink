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

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.table.functions.TableFunction

import _root_.scala.language.implicitConversions

/**
  * == Table API (Scala) ==
  *
  * Importing this package with:
  *
  * {{{
  *   import org.apache.flink.table.api.scala._
  * }}}
  *
  * imports implicit conversions for converting a [[DataSet]] and a [[DataStream]] to a
  * [[Table]]. This can be used to perform SQL-like queries on data. Please have
  * a look at [[Table]] to see which operations are supported and
  * [[org.apache.flink.table.api.scala.ImplicitExpressionOperations]] to see how an
  * expression can be specified.
  *
  * When writing a query you can use Scala Symbols to refer to field names. One would
  * refer to field `a` by writing `'a`. Sometimes it is necessary to manually convert a
  * Scala literal to an Expression literal, in those cases use `Literal`, as in `Literal(3)`.
  *
  * Example:
  *
  * {{{
  *   import org.apache.flink.api.scala._
  *   import org.apache.flink.table.api.scala._
  *
  *   val env = ExecutionEnvironment.getExecutionEnvironment
  *   val tEnv = TableEnvironment.getTableEnvironment(env)
  *
  *   val input: DataSet[(String, Int)] = env.fromElements(("Hello", 2), ("Hello", 5), ("Ciao", 3))
  *   val result = input
  *         .toTable(tEnv, 'word, 'count)
  *         .groupBy('word)
  *         .select('word, 'count.avg)
  *
  *   result.print()
  * }}}
  *
  */
package object scala extends ImplicitExpressionConversions {

  implicit def table2TableConversions(table: Table): TableConversions = {
    new TableConversions(table)
  }

  implicit def dataSet2DataSetConversions[T](set: DataSet[T]): DataSetConversions[T] = {
    new DataSetConversions[T](set, set.getType())
  }

  implicit def table2RowDataSet(table: Table): DataSet[Row] = {
    val tableEnv = table.tableEnv.asInstanceOf[ScalaBatchTableEnv]
    tableEnv.toDataSet[Row](table)
  }

  implicit def dataStream2DataStreamConversions[T](set: DataStream[T]): DataStreamConversions[T] = {
    new DataStreamConversions[T](set, set.dataType)
  }

  implicit def table2RowDataStream(table: Table): DataStream[Row] = {
    val tableEnv = table.tableEnv.asInstanceOf[ScalaStreamTableEnv]
    tableEnv.toAppendStream[Row](table)
  }

  implicit def tableFunctionCall2Table[T](tf: TableFunction[T]): TableFunctionConversions[T] = {
    new TableFunctionConversions[T](tf)
  }
}
