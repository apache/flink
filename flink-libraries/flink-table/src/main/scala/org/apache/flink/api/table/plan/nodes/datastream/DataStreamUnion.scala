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

package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, BiRel}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.TableConfig
import org.apache.flink.streaming.api.datastream.DataStream

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Union.
  *
  */
class DataStreamUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType)
  extends BiRel(cluster, traitSet, left, right)
  with DataStreamRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamUnion(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", unionSelectionToString)
  }

  override def toString = {
    "Union(union: (${rowType.getFieldNames.asScala.toList.mkString(\", \")}))"
  }

  override def translateToPlan(config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataStream[Any] = {

    val leftDataSet = left.asInstanceOf[DataStreamRel].translateToPlan(config)
    val rightDataSet = right.asInstanceOf[DataStreamRel].translateToPlan(config)
    leftDataSet.union(rightDataSet)
  }

  private def unionSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }
}
