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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.codegen.InputFormatCodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.io.CRowValuesInputFormat
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConverters._

/**
  * DataStream RelNode for LogicalValues.
  */
class DataStreamValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    schema: RowSchema,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    ruleDescription: String)
  extends Values(cluster, schema.relDataType, tuples, traitSet)
  with DataStreamRel {

  override def deriveRowType() = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamValues(
      cluster,
      traitSet,
      schema,
      getTuples,
      ruleDescription
    )
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {

    val config = planner.getConfig

    val returnType = CRowTypeInfo(schema.typeInfo)
    val generator = new InputFormatCodeGenerator(config)

    // generate code for every record
    val generatedRecords = getTuples.asScala.map { r =>
      generator.generateResultExpression(
        schema.typeInfo,
        schema.fieldNames,
        r.asScala)
    }

    // generate input format
    val generatedFunction = generator.generateValuesInputFormat(
      ruleDescription,
      generatedRecords.map(_.code),
      schema.typeInfo)

    val inputFormat = new CRowValuesInputFormat(
      generatedFunction.name,
      generatedFunction.code,
      returnType)

    planner.getExecutionEnvironment.createInput(inputFormat, returnType)
  }

}
