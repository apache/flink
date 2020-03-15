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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{DataTypes, Types, ValidationException}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions.utils.ApiExpressionUtils.{typeLiteral, unresolvedCall}
import org.apache.flink.table.expressions.{Expression, PlannerExpressionConverter, ResolvedFieldReference}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST
import org.apache.flink.table.sources.tsextractors.{TimestampExtractor, TimestampExtractorUtils}
import org.apache.flink.table.types.DataType

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.RexNode

import java.util.function.{Function => JFunction}

/** Util class for [[TableSource]]. */
object TableSourceUtil {

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
   * Retrieves an expression to compute a rowtime attribute.
   *
   * @param extractor Timestamp extractor to construct an expression for.
   * @param physicalInputType Physical input type that the timestamp extractor accesses.
   * @param expectedDataType Expected type of the expression. Different in case of batch and
   *                         streaming.
   * @param relBuilder  Builder needed to construct the resulting RexNode.
   * @param nameMapping Additional remapping of a logical to a physical field name.
   *                    TimestampExtractor works with logical names, but accesses physical
   *                    fields
   * @return The [[RexNode]] expression to extract the timestamp.
   */
  def getRowtimeExtractionExpression(
      extractor: TimestampExtractor,
      physicalInputType: DataType,
      expectedDataType: DataType,
      relBuilder: FlinkRelBuilder,
      nameMapping: JFunction[String, String])
    : RexNode = {
    val accessedFields = TimestampExtractorUtils.getAccessedFields(
      extractor,
      physicalInputType,
      nameMapping)

    relBuilder.push(createSchemaRelNode(accessedFields, relBuilder.getCluster))
    val expr = constructExpression(
      expectedDataType,
      extractor,
      accessedFields
    ).accept(PlannerExpressionConverter.INSTANCE)
      .toRexNode(relBuilder)
    relBuilder.clear()
    expr
  }

  private def createSchemaRelNode(
      fields: Array[ResolvedFieldReference],
      cluster: RelOptCluster)
    : RelNode = {
    val maxIdx = fields.map(_.fieldIndex()).max
    val idxMap: Map[Int, (String, TypeInformation[_])] = Map(
      fields.map(f => f.fieldIndex() -> (f.name(), f.resultType())): _*)
    val (physicalFields, physicalTypes) = (0 to maxIdx)
      .map(i => idxMap.getOrElse(i, ("", Types.BYTE))).unzip
    val physicalSchema: RelDataType = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .buildLogicalRowType(
        physicalFields,
        physicalTypes)
    LogicalValues.createEmpty(
      cluster,
      physicalSchema)
  }

  private def constructExpression(
      expectedType: DataType,
      timestampExtractor: TimestampExtractor,
      fieldAccesses: Array[ResolvedFieldReference])
    : Expression = {
    val expression = timestampExtractor.getExpression(fieldAccesses)
    // add cast to requested type and convert expression to RexNode
    // If resultType is TimeIndicatorTypeInfo, its internal format is long, but cast
    // from Timestamp is java.sql.Timestamp. So we need cast to long first.
    unresolvedCall(
      CAST,
      unresolvedCall(CAST, expression, typeLiteral(DataTypes.BIGINT)),
      typeLiteral(expectedType))
  }
}
