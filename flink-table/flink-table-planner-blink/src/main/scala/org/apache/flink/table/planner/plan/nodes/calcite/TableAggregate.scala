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

package org.apache.flink.table.planner.plan.nodes.calcite

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, Pair, Util}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.types.utils.{LegacyTypeInfoDataTypeConverter, TypeConversions}
import org.apache.flink.table.typeutils.FieldInfoUtils

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Relational operator that represents a table aggregate. A TableAggregate is similar to the
  * [[org.apache.calcite.rel.core.Aggregate]] but may output 0 or more records for a group.
  */
abstract class TableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    val aggCalls: util.List[AggregateCall])
  extends SingleRel(cluster, traitSet, input) {

  private[flink] def getGroupSet: ImmutableBitSet = groupSet

  private[flink] def getGroupSets: util.List[ImmutableBitSet] = groupSets

  private[flink] def getAggCallList: util.List[AggregateCall] = aggCalls

  private[flink] def getNamedAggCalls: util.List[Pair[AggregateCall, String]] = {
    getNamedAggCalls(aggCalls, deriveRowType(), groupSet)
  }

  override def deriveRowType(): RelDataType = {
    deriveTableAggRowType(cluster, input, groupSet, aggCalls)
  }

  protected def deriveTableAggRowType(
      cluster: RelOptCluster,
      child: RelNode,
      groupSet: ImmutableBitSet,
      aggCalls: util.List[AggregateCall]): RelDataType = {

    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    val groupNames = new ListBuffer[String]

    // group key fields
    groupSet.asList().foreach(e => {
      val field = child.getRowType.getFieldList.get(e)
      groupNames.append(field.getName)
      builder.add(field)
    })

    // agg fields
    val aggCall = aggCalls.get(0)
    if (aggCall.`type`.isStruct) {
      // only a structured type contains a field list.
      aggCall.`type`.getFieldList.foreach(builder.add)
    } else {
      // A non-structured type does not have a field list, so get field name through
      // FieldInfoUtils.getFieldNames.
      val logicalType = FlinkTypeFactory.toLogicalType(aggCall.`type`)
      val dataType = TypeConversions.fromLogicalToDataType(logicalType)
      val name = FieldInfoUtils
        .getFieldNames(LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType), groupNames).head
      builder.add(name, aggCall.`type`)
    }
    builder.build()
  }

  private[flink] def getNamedAggCalls(
      aggCalls: util.List[AggregateCall],
      rowType: RelDataType,
      groupSet: ImmutableBitSet): util.List[Pair[AggregateCall, String]] = {
    Pair.zip(aggCalls, Util.skip(rowType.getFieldNames, groupSet.cardinality))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("group", groupSet)
      .item("tableAggregate", aggCalls)
  }
}
