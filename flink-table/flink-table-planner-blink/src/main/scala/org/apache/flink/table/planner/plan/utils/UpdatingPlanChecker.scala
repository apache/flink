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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream._

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelNode, RelVisitor}

import scala.collection.JavaConversions._

object UpdatingPlanChecker {

  /** Validates that the plan produces only append changes. */
  def isAppendOnly(plan: RelNode): Boolean = {
    val appendOnlyValidator = new AppendOnlyValidator
    appendOnlyValidator.go(plan)

    appendOnlyValidator.isAppendOnly
  }

  /** Extracts the unique keys of the table produced by the plan. */
  def getUniqueKeyFields(
      relNode: RelNode,
      planner: StreamPlanner,
      sinkFieldNames: Array[String]): Option[Array[Array[String]]] = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null && uniqueKeys.size() > 0) {
      Some(uniqueKeys.filter(_.nonEmpty).map(_.toArray.map(sinkFieldNames)).toArray)
    } else {
      None
    }
  }

  private class AppendOnlyValidator extends RelVisitor {

    var isAppendOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: StreamPhysicalRel if s.producesUpdates || s.producesRetractions =>
          isAppendOnly = false
        case hep: HepRelVertex =>
          visit(hep.getCurrentRel, ordinal, parent)   //remove wrapper node
        case rs: RelSubset =>
          visit(rs.getOriginal, ordinal, parent)      //remove wrapper node
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }
}
