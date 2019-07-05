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

package org.apache.flink.table.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall
import org.apache.flink.table.expressions.{Expression, FieldReferenceExpression, UnresolvedCallExpression, ValueLiteralExpression}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, PreserveWatermarks}
import org.apache.flink.types.Row

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util
import java.util.{Collections, List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object TestTableSources {

  def getPersonCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"),
      "csv-test",
      "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("first", Types.STRING)
      .field("id", Types.INT)
      .field("score",Types.DOUBLE)
      .field("last",Types.STRING)
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .ignoreFirstLine()
      .commentPrefix("%")
      .build()
  }

  def getOrdersCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "2,Euro,2",
      "1,US Dollar,3",
      "50,Yen,4",
      "3,Euro,5",
      "5,US Dollar,6"
    )
    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"),
      "csv-order-test",
      "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("amount", Types.LONG)
      .field("currency", Types.STRING)
      .field("ts",Types.LONG)
      .fieldDelimiter(",")
      .lineDelimiter("$")
      .build()
  }

  def getRatesCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "US Dollar,102",
      "Yen,1",
      "Euro,119",
      "RMB,702"
    )
    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"),
      "csv-rate-test",
      "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("currency", Types.STRING)
      .field("rate", Types.LONG)
      .fieldDelimiter(",")
      .lineDelimiter("$")
      .build()
  }

  private def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}

class TestTableSourceWithTime[T](
    isBatch: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[T],
    rowtime: String = null,
    proctime: String = null,
    mapping: Map[String, String] = null)
  extends StreamTableSource[T]
  with DefinedRowtimeAttributes
  with DefinedProctimeAttribute
  with DefinedFieldMapping {

  override def isBounded: Boolean = isBatch

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    val dataStream = execEnv.fromCollection(values, returnType)
    dataStream.getTransformation.setMaxParallelism(1)
    dataStream
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    // return a RowtimeAttributeDescriptor if rowtime attribute is defined
    if (rowtime != null) {
      Collections.singletonList(new RowtimeAttributeDescriptor(
        rowtime,
        new ExistingField(rowtime),
        new AscendingTimestamps))
    } else {
      Collections.EMPTY_LIST.asInstanceOf[util.List[RowtimeAttributeDescriptor]]
    }
  }

  override def getProctimeAttribute: String = proctime

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def explainSource(): String = ""

  override def getFieldMapping: util.Map[String, String] = {
    if (mapping != null) mapping else null
  }
}

class TestPreserveWMTableSource[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[Either[(Long, T), Long]],
    rowtime: String)
  extends StreamTableSource[T]
  with DefinedRowtimeAttributes {

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    Collections.singletonList(new RowtimeAttributeDescriptor(
      rowtime,
      new ExistingField(rowtime),
      PreserveWatermarks.INSTANCE))
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    execEnv.addSource(new EventTimeSourceFunction[T](values)).
        setParallelism(1).setMaxParallelism(1).returns(returnType)
  }

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def explainSource(): String = ""

}

class TestProjectableTableSource(
    isBatch: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null,
    fieldMapping: Map[String, String] = null)
  extends TestTableSourceWithTime[Row](
    isBatch,
    tableSchema,
    returnType,
    values,
    rowtime,
    proctime,
    fieldMapping)
  with ProjectableTableSource[Row] {

  override def projectFields(fields: Array[Int]): TableSource[Row] = {
    val rowType = returnType.asInstanceOf[RowTypeInfo]

    val (projectedNames: Array[String], projectedMapping) = if (fieldMapping == null) {
      val projectedNames = fields.map(rowType.getFieldNames.apply(_))
      (projectedNames, null)
    } else {
      val invertedMapping = fieldMapping.map(_.swap)
      val projectedNames = fields.map(rowType.getFieldNames.apply(_))

      val projectedMapping: Map[String, String] = projectedNames.map{ f =>
        val logField = invertedMapping(f)
        logField -> s"remapped-$f"
      }.toMap
      val renamedNames = projectedNames.map(f => s"remapped-$f")
      (renamedNames, projectedMapping)
    }

    val projectedTypes = fields.map(rowType.getFieldTypes.apply(_))
    val projectedReturnType = new RowTypeInfo(
      projectedTypes.asInstanceOf[Array[TypeInformation[_]]],
      projectedNames)

    val projectedDataTypes = fields.map(tableSchema.getFieldDataTypes.apply(_))
    val newTableSchema = TableSchema.builder().fields(projectedNames, projectedDataTypes).build()

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    new TestProjectableTableSource(
      isBatch,
      newTableSchema,
      projectedReturnType,
      projectedValues,
      rowtime,
      proctime,
      projectedMapping)
  }

  override def explainSource(): String = {
    s"TestSource(" +
      s"physical fields: ${getReturnType.asInstanceOf[RowTypeInfo].getFieldNames.mkString(", ")})"
  }
}

class TestNestedProjectableTableSource(
    isBatch: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null)
  extends TestTableSourceWithTime[Row](
    isBatch,
    tableSchema,
    returnType,
    values,
    rowtime,
    proctime,
    null)
  with NestedFieldsProjectableTableSource[Row] {

  var readNestedFields: Seq[String] = tableSchema.getFieldNames.map(f => s"$f.*")

  override def projectNestedFields(
      fields: Array[Int],
      nestedFields: Array[Array[String]]): TableSource[Row] = {
    val rowType = returnType.asInstanceOf[RowTypeInfo]

    val projectedNames = fields.map(rowType.getFieldNames.apply(_))
    val projectedTypes = fields.map(rowType.getFieldTypes.apply(_))

    val projectedReturnType = new RowTypeInfo(
      projectedTypes.asInstanceOf[Array[TypeInformation[_]]],
      projectedNames)

    // update read nested fields
    val newReadNestedFields = projectedNames.zip(nestedFields)
      .flatMap(f => f._2.map(n => s"${f._1}.$n"))

    val projectedDataTypes = fields.map(tableSchema.getFieldDataTypes.apply(_))
    val newTableSchema = TableSchema.builder().fields(projectedNames, projectedDataTypes).build()

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    val copy = new TestNestedProjectableTableSource(
      isBatch,
      newTableSchema,
      projectedReturnType,
      projectedValues,
      rowtime,
      proctime)
    copy.readNestedFields = newReadNestedFields

    copy
  }

  override def explainSource(): String = {
    s"TestSource(read nested fields: ${readNestedFields.mkString(", ")})"
  }
}

/**
  * A data source that implements some very basic filtering in-memory in order to test
  * expression push-down logic.
  *
  * @param isBatch whether this is a bounded source
  * @param rowTypeInfo The type info for the rows.
  * @param data The data that filtering is applied to in order to get the final dataset.
  * @param filterableFields The fields that are allowed to be filtered.
  * @param filterPredicates The predicates that should be used to filter.
  * @param filterPushedDown Whether predicates have been pushed down yet.
  */
class TestFilterableTableSource(
    isBatch: Boolean,
    rowTypeInfo: RowTypeInfo,
    data: Seq[Row],
    filterableFields: Set[String] = Set(),
    filterPredicates: Seq[Expression] = Seq(),
    val filterPushedDown: Boolean = false)
  extends StreamTableSource[Row]
    with FilterableTableSource[Row] {

  val fieldNames: Array[String] = rowTypeInfo.getFieldNames

  val fieldTypes: Array[TypeInformation[_]] = rowTypeInfo.getFieldTypes

  override def isBounded: Boolean = isBatch

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection[Row](applyPredicatesToRows(data).asJava, getReturnType)
      .setParallelism(1).setMaxParallelism(1)
  }

  override def explainSource(): String = {
    if (filterPredicates.nonEmpty) {
      s"filter=[${filterPredicates.reduce((l, r) => unresolvedCall(AND, l, r)).toString}]"
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
      isBatch,
      rowTypeInfo,
      data,
      filterableFields,
      predicatesToUse,
      filterPushedDown = true)
  }

  override def isFilterPushedDown: Boolean = filterPushedDown

  private def applyPredicatesToRows(rows: Seq[Row]): Seq[Row] = rows.filter(shouldKeep)

  private def shouldPushDown(expr: Expression): Boolean = {
    expr match {
      case expr: UnresolvedCallExpression if expr.getChildren.size() == 2 => shouldPushDown(expr)
      case _ => false
    }
  }

  private def shouldPushDown(binExpr: UnresolvedCallExpression): Boolean = {
    val children = binExpr.getChildren
    require(children.size() == 2)
    (children.head, children.last) match {
      case (f: FieldReferenceExpression, _: ValueLiteralExpression) =>
        filterableFields.contains(f.getName)
      case (_: ValueLiteralExpression, f: FieldReferenceExpression) =>
        filterableFields.contains(f.getName)
      case (f1: FieldReferenceExpression, f2: FieldReferenceExpression) =>
        filterableFields.contains(f1.getName) && filterableFields.contains(f2.getName)
      case (_, _) => false
    }
  }

  private def shouldKeep(row: Row): Boolean = {
    filterPredicates.isEmpty || filterPredicates.forall {
      case expr: UnresolvedCallExpression if expr.getChildren.size() == 2 =>
        binaryFilterApplies(expr, row)
      case expr => throw new RuntimeException(expr + " not supported!")
    }
  }

  private def binaryFilterApplies(binExpr: UnresolvedCallExpression, row: Row): Boolean = {
    val children = binExpr.getChildren
    require(children.size() == 2)
    val (lhsValue, rhsValue) = extractValues(binExpr, row)

    binExpr.getFunctionDefinition match {
      case BuiltInFunctionDefinitions.GREATER_THAN =>
        lhsValue.compareTo(rhsValue) > 0
      case BuiltInFunctionDefinitions.LESS_THAN =>
        lhsValue.compareTo(rhsValue) < 0
      case BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL =>
        lhsValue.compareTo(rhsValue) >= 0
      case BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL =>
        lhsValue.compareTo(rhsValue) <= 0
      case BuiltInFunctionDefinitions.EQUALS =>
        lhsValue.compareTo(rhsValue) == 0
      case BuiltInFunctionDefinitions.NOT_EQUALS =>
        lhsValue.compareTo(rhsValue) != 0
    }
  }

  private def extractValues(
      binExpr: UnresolvedCallExpression,
      row: Row): (Comparable[Any], Comparable[Any]) = {
    val children = binExpr.getChildren
    require(children.size() == 2)

    (children.head, children.last) match {
      case (l: FieldReferenceExpression, r: ValueLiteralExpression) =>
        val idx = rowTypeInfo.getFieldIndex(l.getName)
        val lv = row.getField(idx).asInstanceOf[Comparable[Any]]
        val rv = getValue(r)
        (lv, rv)
      case (l: ValueLiteralExpression, r: FieldReferenceExpression) =>
        val idx = rowTypeInfo.getFieldIndex(r.getName)
        val lv = getValue(l)
        val rv = row.getField(idx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case (l: ValueLiteralExpression, r: ValueLiteralExpression) =>
        val lv = getValue(l)
        val rv = getValue(r)
        (lv, rv)
      case (l: FieldReferenceExpression, r: FieldReferenceExpression) =>
        val lidx = rowTypeInfo.getFieldIndex(l.getName)
        val ridx = rowTypeInfo.getFieldIndex(r.getName)
        val lv = row.getField(lidx).asInstanceOf[Comparable[Any]]
        val rv = row.getField(ridx).asInstanceOf[Comparable[Any]]
        (lv, rv)
      case _ => throw new RuntimeException(binExpr + " not supported!")
    }
  }

  private def getValue(v: ValueLiteralExpression): Comparable[Any] = {
    val value = v.getValueAs(v.getOutputDataType.getConversionClass)
    if (value.isPresent) {
      value.get().asInstanceOf[Comparable[Any]]
    } else {
      null
    }
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}

object TestFilterableTableSource {

  /**
    * @return The default filterable table source.
    */
  def apply(isBatch: Boolean): TestFilterableTableSource = {
    apply(isBatch, defaultTypeInfo, defaultRows, defaultFilterableFields)
  }

  /**
    * A filterable data source with custom data.
    *
    * @param isBatch whether this is a bounded source
    * @param rowTypeInfo The type of the data. Its expected that both types and field
    *                    names are provided.
    * @param rows The data as a sequence of rows.
    * @param filterableFields The fields that are allowed to be filtered on.
    * @return The table source.
    */
  def apply(
      isBatch: Boolean,
      rowTypeInfo: RowTypeInfo,
      rows: Seq[Row],
      filterableFields: Set[String]): TestFilterableTableSource = {
    new TestFilterableTableSource(isBatch, rowTypeInfo, rows, filterableFields)
  }

  private lazy val defaultFilterableFields = Set("amount")

  private lazy val defaultTypeInfo: RowTypeInfo = {
    val fieldNames: Array[String] = Array("name", "id", "amount", "price")
    val fieldTypes: Array[TypeInformation[_]] =
      Array(Types.STRING, Types.LONG, Types.INT, Types.DOUBLE)
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
