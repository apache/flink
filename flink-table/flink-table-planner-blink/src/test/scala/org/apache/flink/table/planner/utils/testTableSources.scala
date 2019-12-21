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

package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.{CollectionInputFormat, RowCsvInputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, TableEnvironment, TableSchema, Types}
import org.apache.flink.table.catalog.{CatalogPartitionImpl, CatalogPartitionSpec, CatalogTableImpl, ObjectPath}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.{DescriptorProperties, Schema}
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall
import org.apache.flink.table.expressions.{CallExpression, Expression, FieldReferenceExpression, ValueLiteralExpression}
import org.apache.flink.table.factories.{StreamTableSourceFactory, TableSourceFactory}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, PreserveWatermarks}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.types.Row

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util
import java.util.{Collections, function, ArrayList => JArrayList, List => JList, Map => JMap}

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
    override val isBounded: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[T],
    rowtime: String = null,
    proctime: String = null,
    mapping: Map[String, String] = null,
    existingTs: String = null)
  extends StreamTableSource[T]
  with DefinedRowtimeAttributes
  with DefinedProctimeAttribute
  with DefinedFieldMapping {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    val dataStream = execEnv.fromCollection(values, returnType)
    dataStream.getTransformation.setMaxParallelism(1)
    dataStream
  }

  override def getRowtimeAttributeDescriptors: JList[RowtimeAttributeDescriptor] = {
    // return a RowtimeAttributeDescriptor if rowtime attribute is defined
    if (rowtime != null) {
      val existingField = if (existingTs != null) {
        existingTs
      } else {
        rowtime
      }
      Collections.singletonList(new RowtimeAttributeDescriptor(
        rowtime,
        new ExistingField(existingField),
        new AscendingTimestamps))
    } else {
      Collections.EMPTY_LIST.asInstanceOf[JList[RowtimeAttributeDescriptor]]
    }
  }

  override def getProctimeAttribute: String = proctime

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def explainSource(): String = ""

  override def getFieldMapping: JMap[String, String] = {
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
    isBounded: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null,
    fieldMapping: Map[String, String] = null)
  extends TestTableSourceWithTime[Row](
    isBounded,
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

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    new TestProjectableTableSource(
      isBounded,
      tableSchema,
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
    isBounded: Boolean,
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null)
  extends TestTableSourceWithTime[Row](
    isBounded,
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

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    val copy = new TestNestedProjectableTableSource(
      isBounded,
      tableSchema,
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

/** Table source factory to find and create [[TestProjectableTableSource]]. */
class TestProjectableTableSourceFactory extends StreamTableSourceFactory[Row] {
  override def createStreamTableSource(properties: JMap[String, String])
  : StreamTableSource[Row] = {
    val descriptorProps = new DescriptorProperties()
    descriptorProps.putProperties(properties)
    val isBounded = descriptorProps.getBoolean("is-bounded")
    val tableSchema = descriptorProps.getTableSchema(Schema.SCHEMA)
    // Build physical row type.
    val schemaBuilder = TableSchema.builder()
    tableSchema
      .getTableColumns
      .filter(c => !c.isGenerated)
      .foreach(c => schemaBuilder.field(c.getName, c.getType))
    val rowTypeInfo = schemaBuilder.build().toRowType
    new TestProjectableTableSource(isBounded, tableSchema, rowTypeInfo, Seq())
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestProjectableSource")
    context
  }

  override def supportedProperties(): JList[String] = {
    val supported = new JArrayList[String]()
    supported.add("*")
    supported
  }
}

/**
  * A data source that implements some very basic filtering in-memory in order to test
  * expression push-down logic.
  *
  * @param isBounded whether this is a bounded source
  * @param rowTypeInfo The type info for the rows.
  * @param data The data that filtering is applied to in order to get the final dataset.
  * @param filterableFields The fields that are allowed to be filtered.
  * @param filterPredicates The predicates that should be used to filter.
  * @param filterPushedDown Whether predicates have been pushed down yet.
  */
class TestFilterableTableSource(
    override val isBounded: Boolean,
    rowTypeInfo: RowTypeInfo,
    data: Seq[Row],
    filterableFields: Set[String] = Set(),
    filterPredicates: Seq[Expression] = Seq(),
    val filterPushedDown: Boolean = false)
  extends StreamTableSource[Row]
  with FilterableTableSource[Row] {

  val fieldNames: Array[String] = rowTypeInfo.getFieldNames

  val fieldTypes: Array[TypeInformation[_]] = rowTypeInfo.getFieldTypes

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection[Row](applyPredicatesToRows(data).asJava, getReturnType)
      .setParallelism(1).setMaxParallelism(1)
  }

  override def explainSource(): String = {
    if (filterPredicates.nonEmpty) {
      s"filterPushedDown=[$filterPushedDown], " +
      s"filter=[${filterPredicates.reduce((l, r) => unresolvedCall(AND, l, r)).toString}]"
    } else {
      s"filterPushedDown=[$filterPushedDown], filter=[]"
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
      isBounded,
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
      case expr: CallExpression if expr.getChildren.size() == 2 => shouldPushDown(expr)
      case _ => false
    }
  }

  private def shouldPushDown(binExpr: CallExpression): Boolean = {
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
      case expr: CallExpression if expr.getChildren.size() == 2 =>
        binaryFilterApplies(expr, row)
      case expr => throw new RuntimeException(expr + " not supported!")
    }
  }

  private def binaryFilterApplies(binExpr: CallExpression, row: Row): Boolean = {
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
      binExpr: CallExpression,
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
  def apply(isBounded: Boolean): TestFilterableTableSource = {
    apply(isBounded, defaultTypeInfo, defaultRows, defaultFilterableFields)
  }

  /**
    * A filterable data source with custom data.
    *
    * @param isBounded whether this is a bounded source
    * @param rowTypeInfo The type of the data. Its expected that both types and field
    *                    names are provided.
    * @param rows The data as a sequence of rows.
    * @param filterableFields The fields that are allowed to be filtered on.
    * @return The table source.
    */
  def apply(
      isBounded: Boolean,
      rowTypeInfo: RowTypeInfo,
      rows: Seq[Row],
      filterableFields: Set[String]): TestFilterableTableSource = {
    new TestFilterableTableSource(isBounded, rowTypeInfo, rows, filterableFields)
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

/** Table source factory to find and create [[TestFilterableTableSource]]. */
class TestFilterableTableSourceFactory extends StreamTableSourceFactory[Row] {
  override def createStreamTableSource(properties: JMap[String, String])
    : StreamTableSource[Row] = {
    val descriptorProps = new DescriptorProperties()
    descriptorProps.putProperties(properties)
    val isBounded = descriptorProps.getBoolean("is-bounded")
    TestFilterableTableSource.apply(isBounded)
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestFilterableSource")
    context
  }

  override def supportedProperties(): JList[String] = {
    val supported = new JArrayList[String]()
    supported.add("*")
    supported
  }
}

/**
  * A data source that implements some very basic partitionable table source in-memory.
  *
  * @param isBounded whether this is a bounded source
  * @param remainingPartitions remaining partitions after partition pruning
  */
class TestPartitionableTableSource(
    override val isBounded: Boolean,
    remainingPartitions: JList[JMap[String, String]],
    isCatalogTable: Boolean)
  extends StreamTableSource[Row]
  with PartitionableTableSource {

  private val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO)
  // 'part1' and 'part2' are partition fields
  private val fieldNames = Array("id", "name", "part1", "part2")
  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  private val data = mutable.Map[String, Seq[Row]](
    "part1=A,part2=1" -> Seq(row(1, "Anna", "A", 1), row(2, "Jack", "A", 1)),
    "part1=A,part2=2" -> Seq(row(3, "John", "A", 2), row(4, "nosharp", "A", 2)),
    "part1=B,part2=3" -> Seq(row(5, "Peter", "B", 3), row(6, "Lucy", "B", 3)),
    "part1=C,part2=1" -> Seq(row(7, "He", "C", 1), row(8, "Le", "C", 1))
  )

  override def getPartitions: JList[JMap[String, String]] = {
    if (isCatalogTable) {
      throw new RuntimeException("Should not expected.")
    }
    List(
      Map("part1" -> "A", "part2" -> "1").asJava,
      Map("part1" -> "A", "part2" -> "2").asJava,
      Map("part1" -> "B", "part2" -> "3").asJava,
      Map("part1" -> "C", "part2" -> "1").asJava
    ).asJava
  }

  override def applyPartitionPruning(
      remainingPartitions: JList[JMap[String, String]]): TableSource[_] = {
    new TestPartitionableTableSource(isBounded, remainingPartitions, isCatalogTable)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val remainingData = if (remainingPartitions != null) {
      val remainingPartitionList = remainingPartitions.map {
        m => s"part1=${m.get("part1")},part2=${m.get("part2")}"
      }
      data.filterKeys(remainingPartitionList.contains).values.flatten
    } else {
      data.values.flatten
    }

    execEnv.fromCollection[Row](remainingData, getReturnType).setParallelism(1).setMaxParallelism(1)
  }

  override def explainSource(): String = {
    if (remainingPartitions != null) {
      s"partitions=${remainingPartitions.mkString(", ")}"
    } else {
      ""
    }
  }

  override def getReturnType: TypeInformation[Row] = returnType

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}

class TestInputFormatTableSource[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[T]) extends InputFormatTableSource[T] {

  override def getInputFormat: InputFormat[T, _ <: InputSplit] = {
    new CollectionInputFormat[T](values.asJava, returnType.createSerializer(new ExecutionConfig))
  }

  override def getReturnType: TypeInformation[T] =
    throw new RuntimeException("Should not invoke this deprecated method.")

  override def getProducedDataType: DataType = fromLegacyInfoToDataType(returnType)

  override def getTableSchema: TableSchema = tableSchema
}

class TestDataTypeTableSource(
    tableSchema: TableSchema,
    values: Seq[Row]) extends InputFormatTableSource[Row] {

  override def getInputFormat: InputFormat[Row, _ <: InputSplit] = {
    new CollectionInputFormat[Row](
      values.asJava,
      fromDataTypeToTypeInfo(getProducedDataType)
          .createSerializer(new ExecutionConfig)
          .asInstanceOf[TypeSerializer[Row]])
  }

  override def getReturnType: TypeInformation[Row] =
    throw new RuntimeException("Should not invoke this deprecated method.")

  override def getProducedDataType: DataType = tableSchema.toRowDataType

  override def getTableSchema: TableSchema = tableSchema
}

class TestDataTypeTableSourceWithTime(
    tableSchema: TableSchema,
    values: Seq[Row],
    rowtime: String = null)
  extends InputFormatTableSource[Row]
  with DefinedRowtimeAttributes {

  override def getInputFormat: InputFormat[Row, _ <: InputSplit] = {
    new CollectionInputFormat[Row](
      values.asJava,
      fromDataTypeToTypeInfo(getProducedDataType)
        .createSerializer(new ExecutionConfig)
        .asInstanceOf[TypeSerializer[Row]])
  }

  override def getReturnType: TypeInformation[Row] =
    throw new RuntimeException("Should not invoke this deprecated method.")

  override def getProducedDataType: DataType = tableSchema.toRowDataType

  override def getTableSchema: TableSchema = tableSchema

  override def getRowtimeAttributeDescriptors: JList[RowtimeAttributeDescriptor] = {
    // return a RowtimeAttributeDescriptor if rowtime attribute is defined
    if (rowtime != null) {
      Collections.singletonList(new RowtimeAttributeDescriptor(
        rowtime,
        new ExistingField(rowtime),
        new AscendingTimestamps))
    } else {
      Collections.EMPTY_LIST.asInstanceOf[JList[RowtimeAttributeDescriptor]]
    }
  }
}

class TestStreamTableSource(
    tableSchema: TableSchema,
    values: Seq[Row]) extends StreamTableSource[Row] {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection(values, tableSchema.toRowType)
  }

  override def getReturnType: TypeInformation[Row] = tableSchema.toRowType

  override def getTableSchema: TableSchema = tableSchema
}

class TestFileInputFormatTableSource(
    paths: Array[String],
    tableSchema: TableSchema) extends InputFormatTableSource[Row] {

  override def getInputFormat: InputFormat[Row, _ <: InputSplit] = {
    val format = new RowCsvInputFormat(null, tableSchema.getFieldTypes)
    format.setFilePaths(paths: _*)
    format
  }

  override def getReturnType: TypeInformation[Row] = tableSchema.toRowType

  override def getTableSchema: TableSchema = tableSchema
}

class TestPartitionableSourceFactory extends TableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestPartitionableSource")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }

  override def createTableSource(properties: util.Map[String, String]): TableSource[Row] = {
    val dp = new DescriptorProperties()
    dp.putProperties(properties)

    val isBounded = dp.getBoolean("is-bounded")
    val isCatalogTable = dp.getBoolean("is-catalog-table")
    val remainingPartitions = dp.getOptionalArray("remaining-partition",
      new function.Function[String, util.Map[String, String]] {
      override def apply(t: String): util.Map[String, String] = {
        dp.getString(t).split(",")
            .map(kv => kv.split(":"))
            .map(a => (a(0), a(1)))
            .toMap[String, String]
      }
    }).orElse(null)
    new TestPartitionableTableSource(
      isBounded,
      remainingPartitions,
      isCatalogTable)
  }
}

object TestPartitionableSourceFactory {
  private val tableSchema: TableSchema = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("part1", DataTypes.STRING())
    .field("part2", DataTypes.INT())
    .build()

  /**
    * For java invoking.
    */
  def registerTableSource(
      tEnv: TableEnvironment,
      tableName: String,
      isBounded: Boolean): Unit = {
    registerTableSource(tEnv, tableName, isBounded, tableSchema = tableSchema)
  }

  def registerTableSource(
      tEnv: TableEnvironment,
      tableName: String,
      isBounded: Boolean,
      tableSchema: TableSchema = tableSchema,
      remainingPartitions: JList[JMap[String, String]] = null): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("is-bounded", isBounded.toString)
    val isCatalogTable = true
    properties.putBoolean("is-catalog-table", isCatalogTable)
    properties.putString(CONNECTOR_TYPE, "TestPartitionableSource")
    if (remainingPartitions != null) {
      remainingPartitions.zipWithIndex.foreach { case (part, i) =>
        properties.putString(
          "remaining-partition." + i,
          part.map {case (k, v) => s"$k:$v"}.reduce {(kv1, kv2) =>
            s"$kv1,:$kv2"
          }
        )
      }
    }

    val table = new CatalogTableImpl(
      tableSchema,
      util.Arrays.asList[String]("part1", "part2"),
      properties.asMap(),
      ""
    )
    val catalog = tEnv.getCatalog(tEnv.getCurrentCatalog).get()
    val path = new ObjectPath(tEnv.getCurrentDatabase, tableName)
    catalog.createTable(path, table, false)

    if (isCatalogTable) {
      val partitions = List(
        Map("part1" -> "A", "part2" -> "1").asJava,
        Map("part1" -> "A", "part2" -> "2").asJava,
        Map("part1" -> "B", "part2" -> "3").asJava,
        Map("part1" -> "C", "part2" -> "1").asJava
      )
      partitions.foreach(spec => catalog.createPartition(
        path,
        new CatalogPartitionSpec(new java.util.LinkedHashMap(spec)),
        new CatalogPartitionImpl(Map[String, String](), ""),
        true))
    }

  }
}
