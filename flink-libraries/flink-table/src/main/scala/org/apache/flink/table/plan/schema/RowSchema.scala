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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField, RelRecordType}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexShuttle}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TimeMaterializationSqlFunction
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * Schema that describes both a logical and physical row.
  */
class RowSchema(private val logicalRowType: RelDataType) {

  private lazy val physicalRowFields: Seq[RelDataTypeField] = logicalRowType.getFieldList filter {
    field => !FlinkTypeFactory.isTimeIndicatorType(field.getType)
  }

  private lazy val physicalRowType: RelDataType = new RelRecordType(physicalRowFields)

  private lazy val physicalRowFieldTypes: Seq[TypeInformation[_]] = physicalRowFields map { f =>
    FlinkTypeFactory.toTypeInfo(f.getType)
  }

  private lazy val physicalRowFieldNames: Seq[String] = physicalRowFields.map(_.getName)

  private lazy val physicalRowTypeInfo: TypeInformation[Row] = new RowTypeInfo(
    physicalRowFieldTypes.toArray, physicalRowFieldNames.toArray)

  private lazy val indexMapping: Array[Int] = generateIndexMapping

  private lazy val inputRefUpdater = new RexInputRefUpdater()

  private def generateIndexMapping: Array[Int] = {
    val mapping = new Array[Int](logicalRowType.getFieldCount)
    var countTimeIndicators = 0
    var i = 0
    while (i < logicalRowType.getFieldCount) {
      val t = logicalRowType.getFieldList.get(i).getType
      if (FlinkTypeFactory.isTimeIndicatorType(t)) {
        countTimeIndicators += 1
        // no mapping
        mapping(i) = -1
      } else {
        mapping(i) = i - countTimeIndicators
      }
      i += 1
    }
    mapping
  }

  private class RexInputRefUpdater extends RexShuttle {

    override def visitInputRef(inputRef: RexInputRef): RexNode = {
      new RexInputRef(mapIndex(inputRef.getIndex), inputRef.getType)
    }

    override def visitCall(call: RexCall): RexNode = call.getOperator match {
      // we leave time indicators unchanged yet
      // the index becomes invalid but right now we are only
      // interested in the type of the input reference
      case TimeMaterializationSqlFunction => call
      case _ => super.visitCall(call)
    }
  }

  /**
    * Returns the arity of the logical record.
    */
  def logicalArity: Int = logicalRowType.getFieldCount

  /**
    * Returns the arity of the physical record.
    */
  def physicalArity: Int = physicalTypeInfo.getArity

  /**
    * Returns a logical [[RelDataType]] including logical fields (i.e. time indicators).
    */
  def logicalType: RelDataType = logicalRowType

  /**
    * Returns a physical [[RelDataType]] with no logical fields (i.e. time indicators).
    */
  def physicalType: RelDataType = physicalRowType

  /**
    * Returns a physical [[TypeInformation]] of row with no logical fields (i.e. time indicators).
    */
  def physicalTypeInfo: TypeInformation[Row] = physicalRowTypeInfo

  /**
    * Returns [[TypeInformation]] of the row's fields with no logical fields (i.e. time indicators).
    */
  def physicalFieldTypeInfo: Seq[TypeInformation[_]] = physicalRowFieldTypes

  /**
    * Returns the logical fields names including logical fields (i.e. time indicators).
    */
  def logicalFieldNames: Seq[String] = logicalRowType.getFieldNames

  /**
    * Returns the physical fields names with no logical fields (i.e. time indicators).
    */
  def physicalFieldNames: Seq[String] = physicalRowFieldNames

  /**
    * Converts logical indices to physical indices based on this schema.
    */
  def mapIndex(logicalIndex: Int): Int = {
    val mappedIndex = indexMapping(logicalIndex)
    if (mappedIndex < 0) {
      throw new TableException("Invalid access to a logical field.")
    } else {
      mappedIndex
    }
  }

  /**
    * Converts logical indices of a aggregate call to physical ones.
    */
  def mapAggregateCall(logicalAggCall: AggregateCall): AggregateCall = {
    logicalAggCall.copy(
      logicalAggCall.getArgList.map(mapIndex(_).asInstanceOf[Integer]),
      if (logicalAggCall.filterArg < 0) {
        logicalAggCall.filterArg
      } else {
        mapIndex(logicalAggCall.filterArg)
      }
    )
  }

  /**
    * Converts logical field references of a [[RexNode]] to physical ones.
    */
  def mapRexNode(logicalRexNode: RexNode): RexNode = logicalRexNode.accept(inputRefUpdater)

}
