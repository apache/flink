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

package org.apache.flink.table.plan.nodes

import java.util

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.{ImmutableBitSet, Pair, Util}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.table.typeutils.FieldInfoUtils

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

trait CommonTableAggregate extends CommonAggregate {

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
      // TableEnvImpl.getFieldNames.
      val name = FieldInfoUtils
        .getFieldNames(FlinkTypeFactory.toTypeInfo(aggCall.`type`), groupNames).head
      builder.add(name, aggCall.`type`)
    }
    builder.build()
  }

  override private[flink] def aggregationToString(
    inputType: RelDataType,
    grouping: Array[Int],
    rowType: RelDataType,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    namedProperties: Seq[FlinkRelBuilder.NamedWindowProperty]): String = {

    val outFields = rowType.getFieldNames
    val aggOutputType = namedAggregates.head.left.getType
    val tableAggOutputArity = if (aggOutputType.isStruct) {
      aggOutputType.getFieldCount
    } else {
      1
    }
    val groupSize = grouping.length
    val outFieldsOfTableAgg = outFields.subList(groupSize, groupSize + tableAggOutputArity)
    val tableAggOutputFields = Seq(s"(${outFieldsOfTableAgg.mkString(", ")})")

    val newOutFields = outFields.subList(0, groupSize) ++
      tableAggOutputFields ++
      outFields.drop(groupSize + tableAggOutputArity)

    aggregationToString(inputType, grouping, newOutFields, namedAggregates, namedProperties)
  }

  private[flink] def getNamedAggCalls(
    aggCalls: util.List[AggregateCall],
    rowType: RelDataType,
    indicator: Boolean,
    groupSet: ImmutableBitSet)
  : util.List[Pair[AggregateCall, String]] = {

    def getGroupCount: Int = groupSet.cardinality
    def getIndicatorCount: Int = if (indicator) getGroupCount else 0

    val offset = getGroupCount + getIndicatorCount
    Pair.zip(aggCalls, Util.skip(rowType.getFieldNames, offset))
  }
}
