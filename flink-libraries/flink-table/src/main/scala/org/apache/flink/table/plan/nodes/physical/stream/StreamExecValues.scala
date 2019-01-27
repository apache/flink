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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.codegen.ValuesCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.BaseRowSchema

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral

/**
  * DataStream RelNode for LogicalValues.
  */
class StreamExecValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    outputSchema: BaseRowSchema,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    description: String)
  extends Values(cluster, outputSchema.relDataType, tuples, traitSet)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecValues(
      cluster,
      traitSet,
      outputSchema,
      getTuples,
      description
    )
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    if (tableEnv.getConfig.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED)) {
      val inputFormat = ValuesCodeGenerator.generatorInputFormat(
        tableEnv,
        getRowType,
        tuples,
        description)
      val transformation = tableEnv.execEnv.
          createInput(inputFormat, inputFormat.getProducedType).getTransformation
      transformation.setParallelism(getResource.getParallelism)
      transformation.setResources(getResource.getReservedResourceSpec,
        getResource.getPreferResourceSpec)
      transformation
    } else {
      // enable this feature when runtime support do checkpoint when source finished
      throw new TableException("Values source input is not supported currently. Probably " +
        "there is a where condition which always returns false in your query.")
    }
  }
}
