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

package org.apache.flink.table.plan.nodes.datastream

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Union.
  *
  */
class DataStreamUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    schema: RowSchema)
  extends Union(cluster, traitSet, inputs, true)
  with DataStreamRel {

  override def deriveRowType() = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {

    if (!all) {
      throw new TableException("DataStreamUnion only supports UNION ALL.")
    }

    new DataStreamUnion(
      cluster,
      traitSet,
      inputs,
      schema)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union all", unionSelectionToString)
  }

  override def toString = {
    s"Union All(union: (${schema.fieldNames.mkString(", ")}))"
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    getInputs
      .asScala
      .map(_.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig))
      .reduce((dataSetLeft, dataSetRight) => dataSetLeft.union(dataSetRight))
  }

  private def unionSelectionToString: String = {
    schema.fieldNames.mkString(", ")
  }
}
