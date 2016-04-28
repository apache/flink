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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.table.BatchTableEnvironment
import org.apache.flink.api.table.typeutils.TypeConverter._

import scala.collection.JavaConverters._

class DataSetSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    collations: RelCollation,
    rowType2: RelDataType)
  extends SingleRel(cluster, traitSet, inp)
  with DataSetRel{

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode ={
    new DataSetSort(
      cluster,
      traitSet,
      inputs.get(0),
      collations,
      rowType2
    )
  }

  override def translateToPlan(
              tableEnv: BatchTableEnvironment,
              expectedType: Option[TypeInformation[Any]] = None): DataSet[Any] = {

    val config = tableEnv.getConfig

    val inputDS = inp.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val currentParallelism = inputDS.getExecutionEnvironment.getParallelism
    var partitionedDs = if (currentParallelism == 1) {
      inputDS
    } else {
      inputDS.partitionByRange(fieldCollations.map(_._1): _*)
        .withOrders(fieldCollations.map(_._2): _*)
    }

    fieldCollations.foreach { fieldCollation =>
      partitionedDs = partitionedDs.sortPartition(fieldCollation._1, fieldCollation._2)
    }

    val inputType = partitionedDs.getType
    expectedType match {

      case None if config.getEfficientTypeUsage =>
        partitionedDs

      case _ =>
        val determinedType = determineReturnType(
          getRowType,
          expectedType,
          config.getNullCheck,
          config.getEfficientTypeUsage)

        // conversion
        if (determinedType != inputType) {

          val mapFunc = getConversionMapper(config,
            partitionedDs.getType,
            determinedType,
            "DataSetSortConversion",
            getRowType.getFieldNames.asScala
          )

          partitionedDs.map(mapFunc)
        }
        // no conversion necessary, forward
        else {
          partitionedDs
        }
    }
  }

  private def directionToOrder(direction: Direction) = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
    }

  }

  private val fieldCollations = collations.getFieldCollations.asScala
    .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

  private val sortFieldsToString = fieldCollations
    .map(col => s"${rowType2.getFieldNames.get(col._1)} ${col._2.getShortName}" ).mkString(", ")

  override def toString: String = s"Sort(by: $sortFieldsToString)"

  override def explainTerms(pw: RelWriter) : RelWriter = {
    super.explainTerms(pw)
      .item("orderBy", sortFieldsToString)
  }
}
