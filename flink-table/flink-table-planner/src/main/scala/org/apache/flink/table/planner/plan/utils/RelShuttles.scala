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

import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable

import com.google.common.collect.Sets
import org.apache.calcite.plan.ViewExpanders
import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexNode, RexShuttle, RexSubQuery}

import scala.collection.JavaConversions._

class DefaultRelShuttle extends RelHomogeneousShuttle {

  override def visit(rel: RelNode): RelNode = {
    var change = false
    val newInputs = rel.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change) {
      rel.copy(rel.getTraitSet, newInputs)
    } else {
      rel
    }
  }
}

/**
 * Convert all [[QueryOperationCatalogViewTable]]s (including tables in [[RexSubQuery]]) to a
 * relational expression.
 */
class ExpandTableScanShuttle extends RelShuttleImpl {

  /**
   * Override this method to use `replaceInput` method instead of `copy` method if any children
   * change. This will not change any output of LogicalTableScan when LogicalTableScan is replaced
   * with RelNode tree in its RelTable.
   */
  override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
    stack.push(parent)
    try {
      val child2 = child.accept(this)
      if (child2 ne child) {
        parent.replaceInput(i, child2)
      }
      parent
    } finally {
      stack.pop
    }
  }

  override def visit(filter: LogicalFilter): RelNode = {
    val newCondition = filter.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne filter.getCondition) {
      val newFilter = filter.copy(filter.getTraitSet, filter.getInput, newCondition)
      super.visit(newFilter)
    } else {
      super.visit(filter)
    }
  }

  override def visit(project: LogicalProject): RelNode = {
    val shuttle = new ExpandTableScanInSubQueryShuttle
    var changed = false
    val newProjects = project.getProjects.map {
      project =>
        val newProject = project.accept(shuttle)
        if (newProject ne project) {
          changed = true
        }
        newProject
    }
    if (changed) {
      val newProject =
        project.copy(project.getTraitSet, project.getInput, newProjects, project.getRowType)
      super.visit(newProject)
    } else {
      super.visit(project)
    }
  }

  override def visit(join: LogicalJoin): RelNode = {
    val newCondition = join.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne join.getCondition) {
      val newJoin = join.copy(
        join.getTraitSet,
        newCondition,
        join.getLeft,
        join.getRight,
        join.getJoinType,
        join.isSemiJoinDone)
      super.visit(newJoin)
    } else {
      super.visit(join)
    }
  }

  class ExpandTableScanInSubQueryShuttle extends RexShuttle {
    override def visitSubQuery(subQuery: RexSubQuery): RexNode = {
      val newRel = subQuery.rel.accept(ExpandTableScanShuttle.this)
      var changed = false
      val newOperands = subQuery.getOperands.map {
        op =>
          val newOp = op.accept(ExpandTableScanInSubQueryShuttle.this)
          if (op ne newOp) {
            changed = true
          }
          newOp
      }

      var newSubQuery = subQuery
      if (newRel ne newSubQuery.rel) {
        newSubQuery = newSubQuery.clone(newRel)
      }
      if (changed) {
        newSubQuery = newSubQuery.clone(newSubQuery.getType, newOperands)
      }
      newSubQuery
    }
  }

  /**
   * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling
   * [[QueryOperationCatalogViewTable]]#toRel
   */
  override def visit(scan: TableScan): RelNode = {
    scan match {
      case tableScan: LogicalTableScan =>
        val viewTable = tableScan.getTable.unwrap(classOf[QueryOperationCatalogViewTable])
        if (viewTable != null) {
          val rel = viewTable.toRel(ViewExpanders.simpleContext(tableScan.getCluster))
          rel.accept(this)
        } else {
          tableScan
        }
      case otherScan => otherScan
    }
  }
}

/**
 * Rewrite same rel object to different rel objects.
 *
 * <p>e.g.
 * {{{
 *      Join                       Join
 *     /    \                     /    \
 * Filter1 Filter2     =>     Filter1 Filter2
 *     \   /                     |      |
 *      Scan                  Scan1    Scan2
 * }}}
 * After rewrote, Scan1 and Scan2 are different object but have same digest.
 */
class SameRelObjectShuttle extends DefaultRelShuttle {
  private val visitedNodes = Sets.newIdentityHashSet[RelNode]()

  override def visit(node: RelNode): RelNode = {
    val visited = !visitedNodes.add(node)
    var change = false
    val newInputs = node.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change || visited) {
      node.copy(node.getTraitSet, newInputs)
    } else {
      node
    }
  }
}
