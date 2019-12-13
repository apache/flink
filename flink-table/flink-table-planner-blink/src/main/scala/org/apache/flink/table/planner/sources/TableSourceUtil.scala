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
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.{typeLiteral, valueLiteral}
import org.apache.flink.table.expressions.{CallExpression, ResolvedExpression, ResolvedFieldReference}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isAssignable
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.runtime.types.DataTypePrecisionFixer
import org.apache.flink.table.sources.{DefinedFieldMapping, DefinedProctimeAttribute, DefinedRowtimeAttributes, RowtimeAttributeDescriptor, TableSource}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType, TimestampKind, TimestampType, TinyIntType}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.types.logical.RowType.RowField

import java.sql.Timestamp

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/** Util class for [[TableSource]]. */
object TableSourceUtil {

  /**
    * Computes the indices that map the input type of the DataStream to the schema of the table.
    *
    * The mapping is based on the field names and fails if a table field cannot be
    * mapped to a field of the input type.
    *
    * @param tableSource The table source for which the table schema is mapped to the input type.
    * @param rowType Table source table row type
    * @param isStreamTable If this table source is in streaming mode
    * @return An index mapping from input type to table schema.
    */
  def computeIndexMapping(
      tableSource: TableSource[_],
      rowType: RowType,
      isStreamTable: Boolean): Array[Int] = {

    // get rowtime and proctime attributes
    val rowtimeAttributes = getRowtimeAttributes(tableSource)
    val proctimeAttributes = getProctimeAttribute(tableSource)
    // compute mapping of selected fields and time attributes
    val names = rowType.getFieldNames.toArray
    val fieldTypes = rowType
      .getFields
      .map(_.getType)
      .toArray
    val mapping: Array[Int] = fieldTypes.zip(names).map {
      case (_: TimestampType, name: String)
        if proctimeAttributes.contains(name) =>
        if (isStreamTable) {
          TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER
        } else {
          TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER
        }
      case (_: TimestampType, name: String)
        if rowtimeAttributes.contains(name) =>
        if (isStreamTable) {
          TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER
        } else {
          TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER
        }
      case (t: LogicalType, name: String) =>
        // check if field is registered as time indicator
        if (proctimeAttributes.contains(name)) {
          throw new ValidationException(s"Processing time field '$name' has invalid type $t. " +
            s"Processing time attributes must be of TimestampType.")
        }
        if (rowtimeAttributes.contains(name)) {
          throw new ValidationException(s"Rowtime field '$name' has invalid type $t. " +
            s"Rowtime attributes must be of TimestampType.")
        }
        val (physicalName, idx, logicalType) = resolveInputField(name, tableSource)
        // validate that mapped fields are are same type
        if (!isAssignable(logicalType, t)) {
          throw new ValidationException(s"Type $t of table field '$name' does not " +
            s"match with type $logicalType of the field '$physicalName' of the " +
            "TableSource return type.")
        }
        idx
    }

    val inputType = fromDataTypeToLogicalType(tableSource.getProducedDataType)

    // ensure that only one field is mapped to an atomic type
    if (!(inputType.getTypeRoot == LogicalTypeRoot.ROW) && mapping.count(_ >= 0) > 1) {
      throw new ValidationException(
        s"More than one table field matched to atomic input type $inputType.")
    }

    mapping
  }


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
      logicalSchema: RowType): DataType = {

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
    def mapping(physicalName: String): String = tableSource match {
      case ts: DefinedFieldMapping if ts.getFieldMapping != null =>
        // revert key and value, mapping from physical field to logical field
        val map = ts.getFieldMapping.toMap.map(_.swap)
        map(physicalName)
      case _ =>
        physicalName
    }
    val correspondingLogicalType = sourceLogicalType match {
      case outType: RowType =>
        val fieldsDataType = schemaWithoutProctime.getFields.map(f => (f.getName, f.getType)).toMap
        val fields = outType.getFieldNames.map(n =>
          new RowField(n, fieldsDataType(mapping(n)))).asJava
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
    * @param typeFactory    Type factory to create the type
    * @param fieldNameArray Field names to build the row type
    * @param fieldDataTypeArray Field data types to build the row type
    * @param tableSource    The [[TableSource]] to derive time attributes
    * @param streaming      Flag to determine whether the schema of
    *                       a stream or batch table is created
    * @return The schema for the selected fields of the given [[TableSource]]
    */
  def getSourceRowType(
      typeFactory: FlinkTypeFactory,
      fieldNameArray: Array[String],
      fieldDataTypeArray: Array[DataType],
      tableSource: TableSource[_],
      streaming: Boolean): RelDataType = {

    val fieldNames = fieldNameArray
    var fieldTypes = fieldDataTypeArray
      .map(fromDataTypeToLogicalType)

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
    * @param typeFactory Type factory to create the type
    * @param fieldNameArray Field names to build the row type
    * @param fieldDataTypeArray Field data types to build the row type
    * @param watermarkSpec Watermark specifications defined through DDL
    * @param streaming Flag to determine whether the schema of a stream or batch table is created
    * @return The row type for the selected fields of the given [[TableSource]], this type would
    *         also be patched with time attributes defined in the give [[WatermarkSpec]]
    */
  def getSourceRowType(
      typeFactory: FlinkTypeFactory,
      fieldNameArray: Array[String],
      fieldDataTypeArray: Array[DataType],
      watermarkSpec: WatermarkSpec,
      streaming: Boolean): RelDataType = {

    val fieldNames = fieldNameArray
    var fieldTypes = fieldDataTypeArray
      .map(fromDataTypeToLogicalType)

    // patch rowtime field according to WatermarkSpec
    fieldTypes = if (streaming) {
      // TODO: [FLINK-14473] we only support top-level rowtime attribute right now
      val rowtime = watermarkSpec.getRowtimeAttribute
      if (rowtime.contains(".")) {
        throw new ValidationException(
          s"Nested field '$rowtime' as rowtime attribute is not supported right now.")
      }
      val idx = fieldNames.indexOf(rowtime)
      val originalType = fieldTypes(idx).asInstanceOf[TimestampType]
      val rowtimeType = new TimestampType(
        originalType.isNullable,
        TimestampKind.ROWTIME,
        originalType.getPrecision)
      fieldTypes.patch(idx, Seq(rowtimeType), 1)
    } else {
      fieldTypes
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

    if (tableSchema.getWatermarkSpecs.nonEmpty) {
      getSourceRowType(typeFactory, fieldNames, fieldDataTypes, tableSchema.getWatermarkSpecs.head,
        streaming)
    } else if (tableSource.isDefined) {
      getSourceRowType(typeFactory, fieldNames, fieldDataTypes, tableSource.get,
        streaming)
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
    * Obtains the [[RexNode]] expression to extract the rowtime timestamp for a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the expression is extracted.
    * @param rowType The table source table row type
    * @param cluster The [[RelOptCluster]] of the current optimization process.
    * @param relBuilder The [[RelBuilder]] to build the [[RexNode]].
    * @return The [[RexNode]] expression to extract the timestamp of the table source.
    */
  def getRowtimeExtractionExpression(
      tableSource: TableSource[_],
      rowType: RelDataType,
      cluster: RelOptCluster,
      relBuilder: RelBuilder): Option[RexNode] = {

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    /**
      * Creates a RelNode with a schema that corresponds on the given fields
      * Fields for which no information is available, will have default values.
      */
    def createSchemaRelNode(fields: Array[(String, Int, LogicalType)]): RelNode = {
      val maxIdx = fields.map(_._2).max
      val idxMap: Map[Int, (String, LogicalType)] = Map(
        fields.map(f => f._2 ->(f._1, f._3)): _*)
      val (physicalFields, physicalTypes) = (0 to maxIdx)
        .map(i => idxMap.getOrElse(i, ("", new TinyIntType()))).unzip
      val physicalSchema: RelDataType = typeFactory.buildRelNodeRowType(
        physicalFields,
        physicalTypes)
      LogicalValues.create(
        cluster,
        physicalSchema,
        ImmutableList.of().asInstanceOf[ImmutableList[ImmutableList[RexLiteral]]])
    }

    val rowtimeDesc = getRowtimeAttributeDescriptor(tableSource, rowType)
    rowtimeDesc.map { r =>
      val tsExtractor = r.getTimestampExtractor

      val fieldAccesses: Array[ResolvedFieldReference] =
        if (tsExtractor.getArgumentFields.nonEmpty) {
          val resolvedFields = resolveInputFields(tsExtractor.getArgumentFields, tableSource)
          // push an empty values node with the physical schema on the relbuilder
          relBuilder.push(createSchemaRelNode(resolvedFields))
          // get extraction expression
          resolvedFields.map(
            f => new ResolvedFieldReference(
              f._1,
              fromLogicalTypeToTypeInfo(f._3),
              f._2))
        } else {
          new Array[ResolvedFieldReference](0)
        }

      val expression = tsExtractor.getExpression(fieldAccesses)
      // add cast to requested type and convert expression to RexNode
      // blink runner treats numeric types as seconds in the cast of timestamp and numerical types.
      // So we use REINTERPRET_CAST to keep the mills of numeric types.
      val outputType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[Timestamp])
      val castExpression = new CallExpression(
        BuiltInFunctionDefinitions.REINTERPRET_CAST,
        Seq(
          expression.asInstanceOf[ResolvedExpression],
          typeLiteral(outputType),
          valueLiteral(false)),
        outputType)
      val rexExpression = castExpression.accept(new ExpressionConverter(relBuilder))
      relBuilder.clear()
      rexExpression
    }
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

  /**
    * Identifies for a field name of the logical schema, the corresponding physical field in the
    * return type of a [[TableSource]].
    *
    * @param fieldName The logical field to look up.
    * @param tableSource The table source in which to look for the field.
    * @return The name, index, and logical type of the physical field.
    */
  private def resolveInputField(
      fieldName: String,
      tableSource: TableSource[_]): (String, Int, LogicalType) = {

    val returnType = fromDataTypeToLogicalType(tableSource.getProducedDataType)

    /** Look up a field by name in a type information */
    def lookupField(fieldName: String, failMsg: String): (String, Int, LogicalType) = {

      returnType match {
        case rt: RowType =>
          // get and check field index
          val idx = rt.getFieldIndex(fieldName)
          if (idx < 0) {
            throw new ValidationException(failMsg)
          }

          // return field name, index, and field type
          (fieldName, idx, rt.getTypeAt(idx))
        case _ =>
          // no composite type, we return the full atomic type as field
          (fieldName, 0, returnType)
      }
    }

    tableSource match {
      case d: DefinedFieldMapping if d.getFieldMapping != null =>
        // resolve field name in field mapping
        val resolvedFieldName = d.getFieldMapping.get(fieldName)
        if (resolvedFieldName == null) {
          throw new ValidationException(
            s"Field '$fieldName' could not be resolved by the field mapping.")
        }
        // look up resolved field in return type
        lookupField(
          resolvedFieldName,
          s"Table field '$fieldName' was resolved to TableSource return type field " +
            s"'$resolvedFieldName', but field '$resolvedFieldName' was not found in the return " +
            s"type $returnType of the TableSource. " +
            s"Please verify the field mapping of the TableSource.")
      case _ =>
        // look up field in return type
        lookupField(
          fieldName,
          s"Table field '$fieldName' was not found in the return type $returnType of the " +
            s"TableSource.")
    }
  }

  /**
    * Identifies the physical fields in the return type of a [[TableSource]]
    * for a list of field names of the [[TableSource]]'s [[org.apache.flink.table.api.TableSchema]].
    *
    * @param fieldNames The field names to look up.
    * @param tableSource The table source in which to look for the field.
    * @return The name, index, and logical type of the physical field.
    */
  private def resolveInputFields(
      fieldNames: Array[String],
      tableSource: TableSource[_]): Array[(String, Int, LogicalType)] = {
    fieldNames.map(resolveInputField(_, tableSource))
  }
}
