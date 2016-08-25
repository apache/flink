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

package org.apache.flink.api.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.{BatchTableEnvironment, TableException}
import org.apache.flink.api.table.runtime.{CountPartitionFunction, LimitFilterFunction}
import org.apache.flink.api.table.typeutils.TypeConverter._

import scala.collection.JavaConverters._

class DataSetSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    collations: RelCollation,
    rowType2: RelDataType,
    offset: RexNode,
    fetch: RexNode)
  extends SingleRel(cluster, traitSet, inp)
  with DataSetRel {

  private val limitStart: Long = if (offset != null) {
    RexLiteral.intValue(offset)
  } else {
    0L
  }

  private val limitEnd: Long = if (fetch != null) {
    RexLiteral.intValue(fetch) + limitStart
  } else {
    Long.MaxValue
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataSetSort(
      cluster,
      traitSet,
      inputs.get(0),
      collations,
      rowType2,
      offset,
      fetch
    )
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]] = None)
    : DataSet[Any] = {

    if (fieldCollations.isEmpty) {
      throw TableException("Limiting the result without sorting is not allowed " +
        "as it could lead to arbitrary results.")
    }

    val config = tableEnv.getConfig

    val inputDs = inp.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val currentParallelism = inputDs.getExecutionEnvironment.getParallelism
    var partitionedDs = if (currentParallelism == 1) {
      inputDs
    } else {
      inputDs.partitionByRange(fieldCollations.map(_._1): _*)
        .withOrders(fieldCollations.map(_._2): _*)
    }

    fieldCollations.foreach { fieldCollation =>
      partitionedDs = partitionedDs.sortPartition(fieldCollation._1, fieldCollation._2)
    }

    val limitedDs = if (offset == null && fetch == null) {
      partitionedDs
    } else {
      val countFunction = new CountPartitionFunction[Any]

      val partitionCountName = s"prepare offset/fetch"

      val partitionCount = partitionedDs
        .mapPartition(countFunction)
        .name(partitionCountName)

      val broadcastName = "countPartition"

      val limitFunction = new LimitFilterFunction[Any](
        limitStart,
        limitEnd,
        broadcastName)

      val limitName = s"offset: $offsetToString, fetch: $fetchToString"

      partitionedDs
        .filter(limitFunction)
        .name(limitName)
        .withBroadcastSet(partitionCount, broadcastName)
    }

    val inputType = partitionedDs.getType
    expectedType match {

      case None if config.getEfficientTypeUsage =>
        limitedDs

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {

          val mapFunc = getConversionMapper(
            config,
            false,
            partitionedDs.getType,
            determinedType,
            "DataSetSortConversion",
            getRowType.getFieldNames.asScala
          )

          val opName = s"convert: (${rowType.getFieldNames.asScala.toList.mkString(", ")})"

          limitedDs.map(mapFunc).name(opName)
        }
        // no conversion necessary, forward
        else {
          limitedDs
        }
    }
  }

  private def directionToOrder(direction: Direction) = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }

  }

  private val fieldCollations = collations.getFieldCollations.asScala
    .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

  private val sortFieldsToString = fieldCollations
    .map(col => s"${rowType2.getFieldNames.get(col._1)} ${col._2.getShortName}" ).mkString(", ")

  private val offsetToString = s"$offset"

  private val fetchToString = if (limitEnd == Long.MaxValue) {
    "unlimited"
  } else {
    s"$limitEnd"
  }

  override def toString: String =
    s"Sort(by: ($sortFieldsToString), offset: $offsetToString, fetch: $fetchToString)"

  override def explainTerms(pw: RelWriter) : RelWriter = {
    super.explainTerms(pw)
      .item("orderBy", sortFieldsToString)
      .item("offset", offsetToString)
      .item("fetch", fetchToString)
  }
}
