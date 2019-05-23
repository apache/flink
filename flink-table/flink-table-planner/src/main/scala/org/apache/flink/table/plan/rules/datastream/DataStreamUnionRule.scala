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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamUnion
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalUnion
import org.apache.flink.table.plan.schema.RowSchema

import scala.collection.JavaConverters._

class DataStreamUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamUnionRule")
{

  def convert(rel: RelNode): RelNode = {
    val union: FlinkLogicalUnion = rel.asInstanceOf[FlinkLogicalUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)

    val newInputs = union
      .getInputs
      .asScala
      .map(RelOptRule.convert(_, FlinkConventions.DATASTREAM))
      .asJava

    new DataStreamUnion(
      rel.getCluster,
      traitSet,
      newInputs,
      new RowSchema(rel.getRowType))
  }
}

object DataStreamUnionRule {
  val INSTANCE: RelOptRule = new DataStreamUnionRule
}
