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

import org.apache.calcite.plan.{RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelWriter, BiRel, RelNode}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.{TableConfig, Row}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.table.plan.TypeConverter._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.apache.flink.api.table.plan.TypeConverter

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    opName: String,
    joinKeysLeft: Array[Int],
    joinKeysRight: Array[Int],
    joinType: JoinType,
    joinHint: JoinHint,
    func: (TableConfig, TypeInformation[Any], TypeInformation[Any], TypeInformation[Any]) =>
      FlatJoinFunction[Any, Any, Any])
  extends BiRel(cluster, traitSet, left, right)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      opName,
      joinKeysLeft,
      joinKeysRight,
      joinType,
      joinHint,
      func
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(config)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(config)

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val joinFun = func.apply(config, leftDataSet.getType, rightDataSet.getType, returnType)
      leftDataSet.join(rightDataSet).where(joinKeysLeft: _*).equalTo(joinKeysRight: _*)
      .`with`(joinFun).asInstanceOf[DataSet[Any]]
  }

}
