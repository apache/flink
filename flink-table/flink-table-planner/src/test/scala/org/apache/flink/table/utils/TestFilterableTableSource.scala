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
import org.apache.flink.table.expressions._
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

object TestFilterableTableSourceWithoutExplainSourceOverride{

  /**
    * @return The default filterable table source.
    */
  def apply(): TestFilterableTableSourceWithoutExplainSourceOverride = {
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
  : TestFilterableTableSourceWithoutExplainSourceOverride = {
    new TestFilterableTableSourceWithoutExplainSourceOverride(rowTypeInfo, rows, filterableFields)
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
    filterPushedDown: Boolean = false)
  extends TestFilterableTableSourceWithoutExplainSourceOverride(
    rowTypeInfo,
    data,
    filterableFields,
    filterPredicates,
    filterPushedDown
  ) {

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

  override def explainSource(): String = {
    if (filterPredicates.nonEmpty) {
      // TODO we cast to planner expression as a temporary solution to keep the old interfaces
      s"filterPushedDown=[$filterPushedDown], filter=[${filterPredicates.reduce((l, r) =>
        And(l.asInstanceOf[PlannerExpression], r.asInstanceOf[PlannerExpression])).toString}]"
    } else {
      s"filterPushedDown=[$filterPushedDown], filter=[]"
    }
  }
}

/**
  * A [[TestFilterableTableSource]] without explain source override.
  */
class TestFilterableTableSourceWithoutExplainSourceOverride(
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

    new TestFilterableTableSourceWithoutExplainSourceOverride(
      rowTypeInfo,
      data,
      filterableFields,
      predicatesToUse,
      filterPushedDown = true)
  }

  override def isFilterPushedDown: Boolean = filterPushedDown

  private[flink] def applyPredicatesToRows(rows: Seq[Row]): Seq[Row] = {
    rows.filter(shouldKeep)
  }

  private[flink] def shouldPushDown(expr: Expression): Boolean = {
    expr match {
      case binExpr: BinaryComparison => shouldPushDown(binExpr)
      case _ => false
    }
  }

  private[flink] def shouldPushDown(expr: BinaryComparison): Boolean = {
    (expr.left, expr.right) match {
      case (f: PlannerResolvedFieldReference, v: Literal) =>
        filterableFields.contains(f.name)
      case (v: Literal, f: PlannerResolvedFieldReference) =>
        filterableFields.contains(f.name)
      case (f1: PlannerResolvedFieldReference, f2: PlannerResolvedFieldReference) =>
        filterableFields.contains(f1.name) && filterableFields.contains(f2.name)
      case (_, _) => false
    }
  }

  private[flink] def shouldKeep(row: Row): Boolean = {
    filterPredicates.isEmpty || filterPredicates.forall {
      case expr: BinaryComparison => binaryFilterApplies(expr, row)
      case expr => throw new RuntimeException(expr + " not supported!")
    }
  }

  private[flink] def binaryFilterApplies(expr: BinaryComparison, row: Row): Boolean = {
    val (lhsValue, rhsValue) = extractValues(expr, row)

    expr match {
      case _: GreaterThan =>
        lhsValue.compareTo(rhsValue) > 0
      case LessThan(l: PlannerResolvedFieldReference, r: Literal) =>
        lhsValue.compareTo(rhsValue) < 0
      case GreaterThanOrEqual(l: PlannerResolvedFieldReference, r: Literal) =>
        lhsValue.compareTo(rhsValue) >= 0
      case LessThanOrEqual(l: PlannerResolvedFieldReference, r: Literal) =>
        lhsValue.compareTo(rhsValue) <= 0
      case EqualTo(l: PlannerResolvedFieldReference, r: Literal) =>
        lhsValue.compareTo(rhsValue) == 0
      case NotEqualTo(l: PlannerResolvedFieldReference, r: Literal) =>
        lhsValue.compareTo(rhsValue) != 0
    }
  }

  private[flink] def extractValues(expr: BinaryComparison, row: Row)
    : (Comparable[Any], Comparable[Any]) = {

    (expr.left, expr.right) match {
      case (l: PlannerResolvedFieldReference, r: Literal) =>
        val idx = rowTypeInfo.getFieldIndex(l.name)
        val lv = row.getField(idx).asInstanceOf[Comparable[Any]]
        val rv = r.value.asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: Literal, r: PlannerResolvedFieldReference) =>
        val idx = rowTypeInfo.getFieldIndex(r.name)
        val lv = l.value.asInstanceOf[Comparable[Any]]
        val rv = row.getField(idx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: Literal, r: Literal) =>
        val lv = l.value.asInstanceOf[Comparable[Any]]
        val rv = r.value.asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: PlannerResolvedFieldReference, r: PlannerResolvedFieldReference) =>
        val lidx = rowTypeInfo.getFieldIndex(l.name)
        val ridx = rowTypeInfo.getFieldIndex(r.name)
        val lv = row.getField(lidx).asInstanceOf[Comparable[Any]]
        val rv = row.getField(ridx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case _ => throw new RuntimeException(expr + " not supported!")
    }
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}
