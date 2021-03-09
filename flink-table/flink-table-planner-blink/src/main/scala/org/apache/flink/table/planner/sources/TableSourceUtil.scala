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

package org.apache.flink.table.planner.sources

import org.apache.flink.table.api.{DataTypes, TableSchema, ValidationException, WatermarkSpec}
import org.apache.flink.table.expressions.ApiExpressionUtils.{typeLiteral, valueLiteral}
import org.apache.flink.table.expressions.{CallExpression, Expression, ResolvedExpression, ResolvedFieldReference}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.runtime.types.DataTypePrecisionFixer
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.{TimestampExtractor, TimestampExtractorUtils}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType.RowField
import org.apache.flink.table.types.logical._

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import _root_.java.sql.Timestamp
import _root_.java.util.function.{Function => JFunction}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
 * Note: We aim to gradually port the logic in this class to [[DynamicSinkUtils]].
 */
object TableSourceUtil {

  /**
   * Fixes the precision of [[TableSource#getProducedDataType()]] with the given logical schema
   * type. The precision of producedDataType may lose, because the data type may comes from
   * legacy type (e.g. Types.BIG_DEC). However, the precision is important to convert output of
   * source to internal row.
   *
   * @param tableSource the table source
   * @param logicalSchema the logical schema from DDL which carries the correct precisions
   * @return the produced data type with correct precisions.
   */
  def fixPrecisionForProducedDataType(
      tableSource: TableSource[_],
      logicalSchema: RowType)
    : DataType = {

    // remove proctime field from logical schema, because proctime is not in produced data type
    val schemaWithoutProctime = getProctimeAttribute(tableSource) match {
      case Some(proctime) =>
        val fields = logicalSchema
          .getFields
          .filter(f => !f.getName.equals(proctime))
          .asJava
        new RowType(logicalSchema.isNullable, fields)

      case None => logicalSchema
    }

    // get the corresponding logical type according to the layout of source data type
    val sourceLogicalType = fromDataTypeToLogicalType(tableSource.getProducedDataType)
    def mapping(physicalName: String): Option[String] = tableSource match {
      case ts: DefinedFieldMapping if ts.getFieldMapping != null =>
        // revert key and value, mapping from physical field to logical field
        val map = ts.getFieldMapping.toMap.map(_.swap)
        map.get(physicalName)
      case _ =>
        Some(physicalName)
    }

    val correspondingLogicalType = sourceLogicalType match {
      case outType: RowType =>
        val logicalNamesToTypes = schemaWithoutProctime
          .getFields
          .map(f => (f.getName, f.getType))
          .toMap
        val fields = outType.getFields.map(f => {
          val t = mapping(f.getName) match {
            case Some(n) if logicalNamesToTypes.contains(n) =>
              logicalNamesToTypes(n) // find corresponding logical type
            case _ =>
              f.getType // use physical type if logical type can't find
          }
          new RowField(f.getName, t)
        }).asJava
        new RowType(schemaWithoutProctime.isNullable, fields)

      case _ =>
        // atomic output type
        // get the first type of logical schema list, there must be only one type in the list
        schemaWithoutProctime.getFields.get(0).getType
    }

    // fixing the precision
    tableSource
      .getProducedDataType
      .accept(new DataTypePrecisionFixer(correspondingLogicalType))
  }

  /**
    * Returns schema of the selected fields of the given [[TableSource]].
    *
    * <p> The watermark strategy specifications should either come from the [[TableSchema]]
    * or [[TableSource]].
    *
    * @param typeFactory Type factory to create the type
    * @param tableSource Table source to derive watermark strategies
    * @param streaming Flag to determine whether the schema of a stream or batch table is created
    * @return The row type for the selected fields of the given [[TableSource]], this type would
    *         also be patched with time attributes defined in the give [[WatermarkSpec]]
    */
  def getSourceRowTypeFromSource(
      typeFactory: FlinkTypeFactory,
      tableSource: TableSource[_],
      streaming: Boolean): RelDataType = {
    val tableSchema = tableSource.getTableSchema
    val fieldNames = tableSchema.getFieldNames
    val fieldDataTypes = tableSchema.getFieldDataTypes
    var fieldTypes = fieldDataTypes.map(fromDataTypeToLogicalType)

    if (streaming) {
      // adjust the type of time attributes for streaming tables
      val rowtimeAttributes = getRowtimeAttributes(tableSource)
      val proctimeAttributes = getProctimeAttribute(tableSource)

      // patch rowtime fields with time indicator type
      rowtimeAttributes.foreach { rowtimeField =>
        val idx = fieldNames.indexOf(rowtimeField)
        val rowtimeType = new TimestampType(
          true, TimestampKind.ROWTIME, 3)
        fieldTypes = fieldTypes.patch(idx, Seq(rowtimeType), 1)
      }
      // patch proctime field with time indicator type
      proctimeAttributes.foreach { proctimeField =>
        val idx = fieldNames.indexOf(proctimeField)
        val proctimeType = new TimestampType(
          true, TimestampKind.PROCTIME, 3)
        fieldTypes = fieldTypes.patch(idx, Seq(proctimeType), 1)
      }
    }
    typeFactory.buildRelNodeRowType(fieldNames, fieldTypes)
  }

  /**
   * Returns schema of the selected fields of the given [[TableSource]].
   *
   * <p> The watermark strategy specifications should either come from the [[TableSchema]]
   * or [[TableSource]].
   *
   * @param typeFactory Type factory to create the type
   * @param tableSchema Table schema to derive table field names and data types
   * @param tableSource Table source to derive watermark strategies
   * @param streaming Flag to determine whether the schema of a stream or batch table is created
   * @return The row type for the selected fields of the given [[TableSource]], this type would
   *         also be patched with time attributes defined in the give [[WatermarkSpec]]
   */
  def getSourceRowType(
      typeFactory: FlinkTypeFactory,
      tableSchema: TableSchema,
      tableSource: Option[TableSource[_]],
      streaming: Boolean): RelDataType = {

    val fieldNames = tableSchema.getFieldNames
    val fieldDataTypes = tableSchema.getFieldDataTypes

    if (tableSource.isDefined) {
      getSourceRowTypeFromSource(typeFactory, tableSource.get, streaming)
    } else {
      val fieldTypes = fieldDataTypes.map(fromDataTypeToLogicalType)
      typeFactory.buildRelNodeRowType(fieldNames, fieldTypes)
    }
  }

  /**
    * Returns the [[RowtimeAttributeDescriptor]] of a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the [[RowtimeAttributeDescriptor]] is
    *                    returned.
    * @param rowType The table source table row type
    * @return The [[RowtimeAttributeDescriptor]] of the [[TableSource]].
    */
  def getRowtimeAttributeDescriptor(
      tableSource: TableSource[_],
      rowType: RelDataType): Option[RowtimeAttributeDescriptor] = {

    tableSource match {
      case r: DefinedRowtimeAttributes =>
        val descriptors = r.getRowtimeAttributeDescriptors
        if (descriptors.size() == 0) {
          None
        } else if (descriptors.size > 1) {
          throw new ValidationException("Table with has more than a single rowtime attribute..")
        } else {
          // exactly one rowtime attribute descriptor
          val descriptor = descriptors.get(0)
          if (rowType.getFieldNames.contains(descriptor.getAttributeName)) {
            Some(descriptor)
          } else {
            // If the row type fields does not contain the attribute name
            // (i.e. filtered out by the push down rules), returns None.
            None
          }
        }
      case _ => None
    }
  }

  /**
   * Retrieves an expression to compute a rowtime attribute.
   *
   * @param extractor Timestamp extractor to construct an expression for.
   * @param physicalInputType Physical input type that the timestamp extractor accesses.
   * @param relBuilder  Builder needed to construct the resulting RexNode.
   * @param nameMapping Additional remapping of a logical to a physical field name.
   *                    TimestampExtractor works with logical names, but accesses physical
   *                    fields
   * @return The [[RexNode]] expression to extract the timestamp.
   */
  def getRowtimeExtractionExpression(
      extractor: TimestampExtractor,
      physicalInputType: DataType,
      relBuilder: RelBuilder,
      nameMapping: JFunction[String, String])
    : RexNode = {
    val accessedFields = TimestampExtractorUtils.getAccessedFields(
      extractor,
      physicalInputType,
      nameMapping)

    relBuilder.push(createSchemaRelNode(accessedFields, relBuilder.getCluster))
    val expr = constructExpression(
      extractor,
      accessedFields
    ).accept(new ExpressionConverter(relBuilder))
    relBuilder.clear()
    expr
  }

  private def createSchemaRelNode(
      fields: Array[ResolvedFieldReference],
      cluster: RelOptCluster)
    : RelNode = {
    val maxIdx = fields.map(_.fieldIndex()).max
    val idxMap: Map[Int, (String, LogicalType)] = Map(
      fields.map(f => f.fieldIndex() -> (f.name(), fromTypeInfoToLogicalType(f.resultType()))): _*)
    val (physicalFields, physicalTypes) = (0 to maxIdx)
      .map(i => idxMap.getOrElse(i, ("", new TinyIntType()))).unzip
    val physicalSchema: RelDataType = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .buildRelNodeRowType(
        physicalFields,
        physicalTypes)
    LogicalValues.createEmpty(
      cluster,
      physicalSchema)
  }

  private def constructExpression(
      timestampExtractor: TimestampExtractor,
      fieldAccesses: Array[ResolvedFieldReference])
    : Expression = {
    val expression = timestampExtractor.getExpression(fieldAccesses)
    // add cast to requested type and convert expression to RexNode
    // If resultType is TimeIndicatorTypeInfo, its internal format is long, but cast
    // from Timestamp is java.sql.Timestamp. So we need cast to long first.
    val outputType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[Timestamp])
    new CallExpression(
      BuiltInFunctionDefinitions.REINTERPRET_CAST,
      Seq(
        expression.asInstanceOf[ResolvedExpression],
        typeLiteral(outputType),
        valueLiteral(false)),
      outputType)
  }

  /** Returns a list with all rowtime attribute names of the [[TableSource]]. */
  private def getRowtimeAttributes(tableSource: TableSource[_]): Array[String] = {
    tableSource match {
      case r: DefinedRowtimeAttributes =>
        r.getRowtimeAttributeDescriptors.map(_.getAttributeName).toArray
      case _ =>
        Array()
    }
  }

  /** Returns the proctime attribute of the [[TableSource]] if it is defined. */
  private def getProctimeAttribute(tableSource: TableSource[_]): Option[String] = {
    tableSource match {
      case p: DefinedProctimeAttribute if p.getProctimeAttribute != null =>
        Some(p.getProctimeAttribute)
      case _ =>
        None
    }
  }
}
