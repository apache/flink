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

package org.apache.flink.table.utils

import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Types._
import org.apache.flink.table.expressions._
import org.apache.flink.table.sources.{BatchTableSource, FilterableTableSource, StreamTableSource, TableSource}
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * This source can only handle simple comparision with field "amount".
  * Supports ">, <, >=, <=, =, <>" with an integer.
  */
class TestFilterableTableSource(
    val recordNum: Int = 33)
    extends BatchTableSource[Row]
        with StreamTableSource[Row]
        with FilterableTableSource[Row] {

  var filterPushedDown: Boolean = false

  val fieldNames: Array[String] = Array("name", "id", "amount", "price")

  val fieldTypes: Array[TypeInformation[_]] = Array(STRING, LONG, INT, DOUBLE)

  // all predicates with field "amount"
  private var filterPredicates = new mutable.ArrayBuffer[Expression]

  // all comparing values for field "amount"
  private val filterValues = new mutable.ArrayBuffer[Int]

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.fromCollection[Row](generateDynamicCollection().asJava, getReturnType)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection[Row](generateDynamicCollection().asJava, getReturnType)
  }

  override def explainSource(): String = {
    if (filterPredicates.nonEmpty) {
      s"filter=[${filterPredicates.reduce((l, r) => And(l, r)).toString}]"
    } else {
      ""
    }
  }

  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

  override def applyPredicate(predicates: JList[Expression]): TableSource[Row] = {
    val newSource = new TestFilterableTableSource(recordNum)
    newSource.filterPushedDown = true

    val iterator = predicates.iterator()
    while (iterator.hasNext) {
      iterator.next() match {
        case expr: BinaryComparison =>
          (expr.left, expr.right) match {
            case (f: ResolvedFieldReference, v: Literal) if f.name.equals("amount") =>
              newSource.filterPredicates += expr
              newSource.filterValues += v.value.asInstanceOf[Number].intValue()
              iterator.remove()
            case (_, _) =>
          }
      }
    }

    newSource
  }

  override def isFilterPushedDown: Boolean = filterPushedDown

  private def generateDynamicCollection(): Seq[Row] = {
    Preconditions.checkArgument(filterPredicates.length == filterValues.length)

    for {
      cnt <- 0 until recordNum
      if shouldCreateRow(cnt)
    } yield {
      Row.of(
        s"Record_$cnt",
        cnt.toLong.asInstanceOf[Object],
        cnt.toInt.asInstanceOf[Object],
        cnt.toDouble.asInstanceOf[Object])
    }
  }

  private def shouldCreateRow(value: Int): Boolean = {
    filterPredicates.zip(filterValues).forall {
      case (_: GreaterThan, v) =>
        value > v
      case (_: LessThan, v) =>
        value < v
      case (_: GreaterThanOrEqual, v) =>
        value >= v
      case (_: LessThanOrEqual, v) =>
        value <= v
      case (_: EqualTo, v) =>
        value == v
      case (_: NotEqualTo, v) =>
        value != v
      case (expr, _) =>
        throw new RuntimeException(expr + " not supported!")
    }
  }
}

