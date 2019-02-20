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
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.Types._
import org.apache.flink.table.expressions.{FunctionDefinitions, _}
import org.apache.flink.table.sources.{BatchTableSource, FilterableTableSource, StreamTableSource, TableSource}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

object TestFilterableTableSource {

  /**
    * @return The default filterable table source.
    */
  def apply(): TestFilterableTableSource = {
    apply(defaultTypeInfo, defaultRows, defaultFilterableFields)
  }

  /**
    * A filterable data source with custom data.
    * @param rowTypeInfo The type of the data. Its expected that both types and field
    *                    names are provided.
    * @param rows The data as a sequence of rows.
    * @param filterableFields The fields that are allowed to be filtered on.
    * @return The table source.
    */
  def apply(
      rowTypeInfo: RowTypeInfo,
      rows: Seq[Row],
      filterableFields: Set[String])
    : TestFilterableTableSource = {
    new TestFilterableTableSource(rowTypeInfo, rows, filterableFields)
  }

  private lazy val defaultFilterableFields = Set("amount")

  private lazy val defaultTypeInfo: RowTypeInfo = {
    val fieldNames: Array[String] = Array("name", "id", "amount", "price")
    val fieldTypes: Array[TypeInformation[_]] = Array(STRING, LONG, INT, DOUBLE)
    new RowTypeInfo(fieldTypes, fieldNames)
  }

  private lazy val defaultRows: Seq[Row] = {
    for {
      cnt <- 0 until 33
    } yield {
      Row.of(
        s"Record_$cnt",
        cnt.toLong.asInstanceOf[AnyRef],
        cnt.toInt.asInstanceOf[AnyRef],
        cnt.toDouble.asInstanceOf[AnyRef])
    }
  }
}

/**
  * A data source that implements some very basic filtering in-memory in order to test
  * expression push-down logic.
  *
  * @param rowTypeInfo The type info for the rows.
  * @param data The data that filtering is applied to in order to get the final dataset.
  * @param filterableFields The fields that are allowed to be filtered.
  * @param filterPredicates The predicates that should be used to filter.
  * @param filterPushedDown Whether predicates have been pushed down yet.
  */
class TestFilterableTableSource(
    rowTypeInfo: RowTypeInfo,
    data: Seq[Row],
    filterableFields: Set[String] = Set(),
    filterPredicates: Seq[Expression] = Seq(),
    val filterPushedDown: Boolean = false)
  extends BatchTableSource[Row]
    with StreamTableSource[Row]
    with FilterableTableSource[Row] {

  val fieldNames: Array[String] = rowTypeInfo.getFieldNames

  val fieldTypes: Array[TypeInformation[_]] = rowTypeInfo.getFieldTypes

  // all comparing values for field "amount"
  private val filterValues = new mutable.ArrayBuffer[Int]

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.fromCollection[Row](applyPredicatesToRows(data).asJava, getReturnType)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection[Row](applyPredicatesToRows(data).asJava, getReturnType)
  }

  override def explainSource(): String = {
    if (filterPredicates.nonEmpty) {
      s"filter=[${
        filterPredicates
          .reduce((l, r) => ExpressionUtils.call(FunctionDefinitions.AND, Seq(l, r))).toString
      }]"
    } else {
      ""
    }
  }

  override def getReturnType: TypeInformation[Row] = rowTypeInfo

  override def applyPredicate(predicates: JList[Expression]): TableSource[Row] = {
    val predicatesToUse = new mutable.ListBuffer[Expression]()
    val iterator = predicates.iterator()
    while (iterator.hasNext) {
      val expr = iterator.next()
      if (shouldPushDown(expr)) {
        predicatesToUse += expr
        iterator.remove()
      }
    }

    new TestFilterableTableSource(
      rowTypeInfo,
      data,
      filterableFields,
      predicatesToUse,
      filterPushedDown = true)
  }

  override def isFilterPushedDown: Boolean = filterPushedDown

  private def applyPredicatesToRows(rows: Seq[Row]): Seq[Row] = {
    rows.filter(shouldKeep)
  }

  private def shouldPushDown(expr: Expression): Boolean = {
    expr match {
      case call: CallExpression
        if call.getFunctionDefinition.getFunctionType == FunctionType.BINARY_COMPARISON =>
        shouldPushDown(call)
      case _ => false
    }
  }

  private def shouldPushDown(binaryComparison: CallExpression): Boolean = {
    (binaryComparison.getChildren.get(0), binaryComparison.getChildren.get(1)) match {
      case (f: FieldReferenceExpression, v: ValueLiteralExpression) =>
        filterableFields.contains(f.getName)
      case (v: ValueLiteralExpression, f: FieldReferenceExpression) =>
        filterableFields.contains(f.getName)
      case (f1: FieldReferenceExpression, f2: FieldReferenceExpression) =>
        filterableFields.contains(f1.getName) && filterableFields.contains(f2.getName)
      case (_, _) => false
    }
  }

  private def shouldKeep(row: Row): Boolean = {
    filterPredicates.isEmpty || filterPredicates.forall {
      case expr: CallExpression
        if expr.getFunctionDefinition.getFunctionType == FunctionType.BINARY_COMPARISON =>
        binaryFilterApplies(expr, row)
      case expr => throw new RuntimeException(expr + " not supported!")
    }
  }

  private def binaryFilterApplies(binaryComparison: CallExpression, row: Row): Boolean = {
    val (lhsValue, rhsValue) = extractValues(binaryComparison, row)
    val left = binaryComparison.getChildren.get(0)
    val right = binaryComparison.getChildren.get(1)

    binaryComparison.getFunctionDefinition match {
      case FunctionDefinitions.GREATER_THAN =>
        lhsValue.compareTo(rhsValue) > 0
      case FunctionDefinitions.LESS_THAN if left.isInstanceOf[FieldReferenceExpression]
          && right.isInstanceOf[ValueLiteralExpression] =>
        lhsValue.compareTo(rhsValue) < 0
      case FunctionDefinitions.GREATER_THAN_OR_EQUAL if left.isInstanceOf[FieldReferenceExpression]
          && right.isInstanceOf[ValueLiteralExpression] =>
        lhsValue.compareTo(rhsValue) >= 0
      case FunctionDefinitions.LESS_THAN_OR_EQUAL if left.isInstanceOf[FieldReferenceExpression]
          && right.isInstanceOf[ValueLiteralExpression] =>
        lhsValue.compareTo(rhsValue) <= 0
      case FunctionDefinitions.EQUALS if left.isInstanceOf[FieldReferenceExpression]
          && right.isInstanceOf[ValueLiteralExpression] =>
        lhsValue.compareTo(rhsValue) == 0
      case FunctionDefinitions.NOT_EQUALS if left.isInstanceOf[FieldReferenceExpression]
        && right.isInstanceOf[ValueLiteralExpression] =>
        lhsValue.compareTo(rhsValue) != 0
    }
  }

  private def extractValues(binaryComparison: CallExpression, row: Row)
    : (Comparable[Any], Comparable[Any]) = {
    val left = binaryComparison.getChildren.get(0)
    val right = binaryComparison.getChildren.get(1)

    (left, right) match {
      case (l: FieldReferenceExpression, r: ValueLiteralExpression) =>
        val idx = rowTypeInfo.getFieldIndex(l.getName)
        val lv = row.getField(idx).asInstanceOf[Comparable[Any]]
        val rv = r.getValue.asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: ValueLiteralExpression, r: FieldReferenceExpression) =>
        val idx = rowTypeInfo.getFieldIndex(r.getName)
        val lv = l.getValue.asInstanceOf[Comparable[Any]]
        val rv = row.getField(idx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: ValueLiteralExpression, r: ValueLiteralExpression) =>
        val lv = l.getValue.asInstanceOf[Comparable[Any]]
        val rv = r.getValue.asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: FieldReferenceExpression, r: FieldReferenceExpression) =>
        val lidx = rowTypeInfo.getFieldIndex(l.getName)
        val ridx = rowTypeInfo.getFieldIndex(r.getName)
        val lv = row.getField(lidx).asInstanceOf[Comparable[Any]]
        val rv = row.getField(ridx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case _ => throw new RuntimeException(binaryComparison + " not supported!")
    }
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}
