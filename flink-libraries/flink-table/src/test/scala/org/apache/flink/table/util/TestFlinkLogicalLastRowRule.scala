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

package org.apache.flink.table.util


import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.logical._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * This rule is used during test and to generate FlinkLogicalLastRow logical node after
  * source. Note: only use the index 2 as a unique key column.
  */
class TestFlinkLogicalLastRowRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalRel],
      operand(classOf[FlinkLogicalTableSourceScan], any())),
    "FlinkLogicalLastRowRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel: FlinkLogicalRel = call.rel(0)
    !rel.isInstanceOf[FlinkLogicalLastRow]
  }

  def getChildRelNodes(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.asScala.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {

    val rel: FlinkLogicalRel = call.rel[FlinkLogicalRel](0)
    val scan: FlinkLogicalTableSourceScan = call.rel[FlinkLogicalTableSourceScan](1)

    val lastRowNode = new FlinkLogicalLastRow(
      rel.getCluster, scan.getTraitSet, scan, Array(1), scan.getRowType)

    val relInputs = getChildRelNodes(rel)
    val newRelInputs = for (c <- relInputs) yield {
      if (c.equals(scan)) {
        lastRowNode
      } else {
        c
      }
    }

    val newRel = rel.copy(rel.getTraitSet, newRelInputs)
    call.transformTo(newRel)
  }
}

object TestFlinkLogicalLastRowRule {
  val INSTANCE = new TestFlinkLogicalLastRowRule
}


