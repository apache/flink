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

package org.apache.flink.table.sources

import java.sql.Timestamp
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.{DataTypes, TableException, Types, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.{typeLiteral, unresolvedCall}
import org.apache.flink.table.expressions.{PlannerExpressionConverter, ResolvedFieldReference}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST
import org.apache.flink.table.types.utils.TypeConversions.{fromDataTypeToLegacyInfo, fromLegacyInfoToDataType}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import scala.collection.JavaConverters._

/** Util class for [[TableSource]]. */
object TableSourceUtil {

  /** Returns true if the [[TableSource]] has a rowtime attribute. */
  def hasRowtimeAttribute(tableSource: TableSource[_]): Boolean =
    getRowtimeAttributes(tableSource).nonEmpty

  /** Returns true if the [[TableSource]] has a proctime attribute. */
  def hasProctimeAttribute(tableSource: TableSource[_]): Boolean =
    getProctimeAttribute(tableSource).nonEmpty

  /**
    * Computes the indices that map the input type of the DataStream to the schema of the table.
    *
    * The mapping is based on the field names and fails if a table field cannot be
    * mapped to a field of the input type.
    *
    * @param tableSource The table source for which the table schema is mapped to the input type.
    * @param isStreamTable True if the mapping is computed for a streaming table, false otherwise.
    * @param selectedFields The indexes of the table schema fields for which a mapping is
    *                       computed. If None, a mapping for all fields is computed.
    * @return An index mapping from input type to table schema.
    */
  def computeIndexMapping(
      tableSource: TableSource[_],
      isStreamTable: Boolean,
      selectedFields: Option[Array[Int]]): Array[Int] = {
    val inputType = fromDataTypeToLegacyInfo(tableSource.getProducedDataType)
    val tableSchema = tableSource.getTableSchema

    // get names of selected fields
    val tableFieldNames = if (selectedFields.isDefined) {
      val names = tableSchema.getFieldNames
      selectedFields.get.map(names(_))
    } else {
      tableSchema.getFieldNames
    }

    // get types of selected fields
    val tableFieldTypes = if (selectedFields.isDefined) {
      val types = tableSchema.getFieldTypes
      selectedFields.get.map(types(_))
    } else {
      tableSchema.getFieldTypes
    }

    // get rowtime and proctime attributes
    val rowtimeAttributes = getRowtimeAttributes(tableSource)
    val proctimeAttributes = getProctimeAttribute(tableSource)

    // compute mapping of selected fields and time attributes
    val mapping: Array[Int] = tableFieldTypes.zip(tableFieldNames).map {
      case (t: SqlTimeTypeInfo[_], name: String)
        if t.getTypeClass == classOf[Timestamp] && proctimeAttributes.contains(name) =>
        if (isStreamTable) {
          TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER
        } else {
          TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER
        }
      case (t: SqlTimeTypeInfo[_], name: String)
        if t.getTypeClass == classOf[Timestamp] && rowtimeAttributes.contains(name) =>
        if (isStreamTable) {
          TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER
        } else {
          TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER
        }
      case (t: TypeInformation[_], name) =>
        // check if field is registered as time indicator
        if (getProctimeAttribute(tableSource).contains(name)) {
          throw new ValidationException(s"Processing time field '$name' has invalid type $t. " +
            s"Processing time attributes must be of type ${Types.SQL_TIMESTAMP}.")
        }
        if (getRowtimeAttributes(tableSource).contains(name)) {
          throw new ValidationException(s"Rowtime field '$name' has invalid type $t. " +
            s"Rowtime attributes must be of type ${Types.SQL_TIMESTAMP}.")
        }

        val (physicalName, idx, tpe) = resolveInputField(name, tableSource)
        // validate that mapped fields are are same type
        if (tpe != t) {
          throw new ValidationException(s"Type $t of table field '$name' does not " +
            s"match with type $tpe of the field '$physicalName' of the TableSource return type.")
        }
        idx
    }

    // ensure that only one field is mapped to an atomic type
    if (!inputType.isInstanceOf[CompositeType[_]] && mapping.count(_ >= 0) > 1) {
      throw new ValidationException(
        s"More than one table field matched to atomic input type $inputType.")
    }

    mapping
  }

  /**
    * Returns the Calcite schema of a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the Calcite schema is generated.
    * @param selectedFields The indices of all selected fields. None, if all fields are selected.
    * @param streaming Flag to determine whether the schema of a stream or batch table is created.
    * @param typeFactory The type factory to create the schema.
    * @return The Calcite schema for the selected fields of the given [[TableSource]].
    */
  def getRelDataType(
      tableSource: TableSource[_],
      selectedFields: Option[Array[Int]],
      streaming: Boolean,
      typeFactory: FlinkTypeFactory): RelDataType = {

    val fieldNames = tableSource.getTableSchema.getFieldNames
    var fieldTypes = tableSource.getTableSchema.getFieldTypes

    if (streaming) {
      // adjust the type of time attributes for streaming tables
      val rowtimeAttributes = getRowtimeAttributes(tableSource)
      val proctimeAttributes = getProctimeAttribute(tableSource)

      // patch rowtime fields with time indicator type
      rowtimeAttributes.foreach { rowtimeField =>
        val idx = fieldNames.indexOf(rowtimeField)
        fieldTypes = fieldTypes.patch(idx, Seq(TimeIndicatorTypeInfo.ROWTIME_INDICATOR), 1)
      }
      // patch proctime field with time indicator type
      proctimeAttributes.foreach { proctimeField =>
        val idx = fieldNames.indexOf(proctimeField)
        fieldTypes = fieldTypes.patch(idx, Seq(TimeIndicatorTypeInfo.PROCTIME_INDICATOR), 1)
      }
    }

    val (selectedFieldNames, selectedFieldTypes) = if (selectedFields.isDefined) {
      // filter field names and types by selected fields
      (selectedFields.get.map(fieldNames(_)), selectedFields.get.map(fieldTypes(_)))
    } else {
      (fieldNames, fieldTypes)
    }
    typeFactory.buildLogicalRowType(selectedFieldNames, selectedFieldTypes)
  }

  /**
    * Returns the [[RowtimeAttributeDescriptor]] of a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the [[RowtimeAttributeDescriptor]] is
    *                    returned.
    * @param selectedFields The fields which are selected from the [[TableSource]].
    *                       If None, all fields are selected.
    * @return The [[RowtimeAttributeDescriptor]] of the [[TableSource]].
    */
  def getRowtimeAttributeDescriptor(
      tableSource: TableSource[_],
      selectedFields: Option[Array[Int]]): Option[RowtimeAttributeDescriptor] = {

    tableSource match {
      case r: DefinedRowtimeAttributes =>
        val descriptors = r.getRowtimeAttributeDescriptors
        if (descriptors.size() == 0) {
          None
        } else if (descriptors.size > 1) {
          throw new ValidationException("Table with has more than a single rowtime attribute.")
        } else {
          // exactly one rowtime attribute descriptor
          if (selectedFields.isEmpty) {
            // all fields are selected.
            Some(descriptors.get(0))
          } else {
            val descriptor = descriptors.get(0)
            // look up index of row time attribute in schema
            val fieldIdx = tableSource.getTableSchema.getFieldNames.indexOf(
              descriptor.getAttributeName)
            // is field among selected fields?
            if (selectedFields.get.contains(fieldIdx)) {
              Some(descriptor)
            } else {
              None
            }
          }
        }
      case _ => None
    }
  }

  /**
    * Obtains the [[RexNode]] expression to extract the rowtime timestamp for a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the expression is extracted.
    * @param selectedFields The selected fields of the [[TableSource]].
    *                       If None, all fields are selected.
    * @param cluster The [[RelOptCluster]] of the current optimization process.
    * @param relBuilder The [[RelBuilder]] to build the [[RexNode]].
    * @param resultType The result type of the timestamp expression.
    * @return The [[RexNode]] expression to extract the timestamp of the table source.
    */
  def getRowtimeExtractionExpression(
      tableSource: TableSource[_],
      selectedFields: Option[Array[Int]],
      cluster: RelOptCluster,
      relBuilder: RelBuilder,
      resultType: TypeInformation[_]): Option[RexNode] = {

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    /**
      * Creates a RelNode with a schema that corresponds on the given fields
      * Fields for which no information is available, will have default values.
      */
    def createSchemaRelNode(fields: Array[(String, Int, TypeInformation[_])]): RelNode = {
      val maxIdx = fields.map(_._2).max
      val idxMap: Map[Int, (String, TypeInformation[_])] = Map(
        fields.map(f => f._2 -> (f._1, f._3)): _*)
      val (physicalFields, physicalTypes) = (0 to maxIdx)
        .map(i => idxMap.getOrElse(i, ("", Types.BYTE))).unzip
      val physicalSchema: RelDataType = typeFactory.buildLogicalRowType(
        physicalFields,
        physicalTypes)
      LogicalValues.create(
        cluster,
        physicalSchema,
        ImmutableList.of().asInstanceOf[ImmutableList[ImmutableList[RexLiteral]]])
    }

    val rowtimeDesc = getRowtimeAttributeDescriptor(tableSource, selectedFields)
    rowtimeDesc.map { r =>
      val tsExtractor = r.getTimestampExtractor

      val fieldAccesses = if (tsExtractor.getArgumentFields.nonEmpty) {
        val resolvedFields = resolveInputFields(tsExtractor.getArgumentFields, tableSource)
        // push an empty values node with the physical schema on the relbuilder
        relBuilder.push(createSchemaRelNode(resolvedFields))
        // get extraction expression
        resolvedFields.map(f => new ResolvedFieldReference(f._1, f._3, f._2))
      } else {
        new Array[ResolvedFieldReference](0)
      }

      val expression = tsExtractor.getExpression(fieldAccesses)
      // add cast to requested type and convert expression to RexNode
      // If resultType is TimeIndicatorTypeInfo, its internal format is long, but cast
      // from Timestamp is java.sql.Timestamp. So we need cast to long first.
      val castExpression = unresolvedCall(CAST,
        unresolvedCall(CAST, expression, typeLiteral(DataTypes.BIGINT())),
        typeLiteral(fromLegacyInfoToDataType(resultType)))

      // TODO we convert to planner expressions as a temporary solution
      val rexExpression = castExpression
          .accept(PlannerExpressionConverter.INSTANCE)
          .toRexNode(relBuilder)
      relBuilder.clear()
      rexExpression
    }
  }

  /**
    * Returns the indexes of the physical fields that required to compute the given logical fields.
    *
    * @param tableSource The [[TableSource]] for which the physical indexes are computed.
    * @param logicalFieldIndexes The indexes of the accessed logical fields for which the physical
    *                            indexes are computed.
    * @return The indexes of the physical fields are accessed to forward and compute the logical
    *         fields.
    */
  def getPhysicalIndexes(
      tableSource: TableSource[_],
      logicalFieldIndexes: Array[Int]): Array[Int] = {

    // get the mapping from logical to physical positions.
    // stream / batch distinction not important here
    val fieldMapping = computeIndexMapping(tableSource, isStreamTable = true, None)

    logicalFieldIndexes
      // resolve logical indexes to physical indexes
      .map(fieldMapping(_))
      // resolve time indicator markers to physical indexes
      .flatMap {
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        // proctime field do not access a physical field
        Seq()
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER =>
        // rowtime field is computed.
        // get names of fields which are accessed by the expression to compute the rowtime field.
        val rowtimeAttributeDescriptor = getRowtimeAttributeDescriptor(tableSource, None)
        val accessedFields = if (rowtimeAttributeDescriptor.isDefined) {
          rowtimeAttributeDescriptor.get.getTimestampExtractor.getArgumentFields
        } else {
          throw new TableException("Computed field mapping includes a rowtime marker but the " +
            "TableSource does not provide a RowtimeAttributeDescriptor. " +
            "This is a bug and should be reported.")
        }
        // resolve field names to physical fields
        resolveInputFields(accessedFields, tableSource).map(_._2)
      case idx =>
        Seq(idx)
    }
  }

  /** Returns a list with all rowtime attribute names of the [[TableSource]]. */
  private def getRowtimeAttributes(tableSource: TableSource[_]): Array[String] = {
    tableSource match {
      case r: DefinedRowtimeAttributes =>
        r.getRowtimeAttributeDescriptors.asScala.map(_.getAttributeName).toArray
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
    * @return The name, index, and type information of the physical field.
    */
  private def resolveInputField(
      fieldName: String,
      tableSource: TableSource[_]): (String, Int, TypeInformation[_]) = {

    val returnType = fromDataTypeToLegacyInfo(tableSource.getProducedDataType)

    /** Look up a field by name in a type information */
    def lookupField(fieldName: String, failMsg: String): (String, Int, TypeInformation[_]) = {
      returnType match {

        case c: CompositeType[_] =>
          // get and check field index
          val idx = c.getFieldIndex(fieldName)
          if (idx < 0) {
            throw new ValidationException(failMsg)
          }
          // return field name, index, and field type
          (fieldName, idx, c.getTypeAt(idx))

        case t: TypeInformation[_] =>
          // no composite type, we return the full atomic type as field
          (fieldName, 0, t)
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
    * Identifies the physical fields in the return type [[TypeInformation]] of a [[TableSource]]
    * for a list of field names of the [[TableSource]]'s [[org.apache.flink.table.api.TableSchema]].
    *
    * @param fieldNames The field names to look up.
    * @param tableSource The table source in which to look for the field.
    * @return The name, index, and type information of the physical field.
    */
  private def resolveInputFields(
      fieldNames: Array[String],
      tableSource: TableSource[_]): Array[(String, Int, TypeInformation[_])] = {
    fieldNames.map(resolveInputField(_, tableSource))
  }

}
