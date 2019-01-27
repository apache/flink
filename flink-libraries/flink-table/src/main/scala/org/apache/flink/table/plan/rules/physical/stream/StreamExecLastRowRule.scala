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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalLastRow
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecLastRow
import org.apache.flink.table.plan.schema.BaseRowSchema

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import scala.collection.JavaConversions._

class StreamExecLastRowRule
  extends ConverterRule(
    classOf[FlinkLogicalLastRow],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecLastRowRule") {

  def convert(rel: RelNode): RelNode = {
    val appToUpdate: FlinkLogicalLastRow = rel.asInstanceOf[FlinkLogicalLastRow]

    val requiredDistribution =
      FlinkRelDistribution.hash(appToUpdate.getUniqueKeys.map(e => Integer.valueOf(e)).toSeq)
    val requiredTraitSet = appToUpdate.getInput.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convInput: RelNode = RelOptRule.convert(appToUpdate.getInput, requiredTraitSet)

    new StreamExecLastRow(
      rel.getCluster,
      appToUpdate.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
      convInput,
      new BaseRowSchema(appToUpdate.getInput.getRowType),
      new BaseRowSchema(rel.getRowType),
      appToUpdate.getUniqueKeys,
      description)
  }
}

object StreamExecLastRowRule {
  val INSTANCE = new StreamExecLastRowRule
}

