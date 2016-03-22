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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.StreamTableEnvironment
import org.apache.flink.api.table.plan.nodes.dataset.ValuesInputFormat
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter._
import org.apache.flink.streaming.api.datastream.DataStream


/**
  * DataStream RelNode for LogicalValues.
  */
class DataStreamValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]])
  extends Values(cluster, rowType, tuples, traitSet)
  with DataStreamRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamValues(
      cluster,
      traitSet,
      rowType,
      tuples
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]]) : DataStream[Any] = {

    val config = tableEnv.getConfig

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage).asInstanceOf[RowTypeInfo]

    val inputFormat = new ValuesInputFormat(tuples)

    tableEnv.createDataStreamSource(inputFormat, returnType).asInstanceOf[DataStream[Any]]

  }

}
