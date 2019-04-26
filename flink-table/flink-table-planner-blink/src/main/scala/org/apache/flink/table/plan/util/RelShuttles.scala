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
package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.schema.RelTable

import com.google.common.collect.Sets
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle, RelShuttleImpl}

import scala.collection.JavaConversions._

class DefaultRelShuttle extends RelShuttle {

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

  override def visit(intersect: LogicalIntersect): RelNode = visit(intersect.asInstanceOf[RelNode])

  override def visit(union: LogicalUnion): RelNode = visit(union.asInstanceOf[RelNode])

  override def visit(aggregate: LogicalAggregate): RelNode = visit(aggregate.asInstanceOf[RelNode])

  override def visit(minus: LogicalMinus): RelNode = visit(minus.asInstanceOf[RelNode])

  override def visit(sort: LogicalSort): RelNode = visit(sort.asInstanceOf[RelNode])

  override def visit(`match`: LogicalMatch): RelNode = visit(`match`.asInstanceOf[RelNode])

  override def visit(exchange: LogicalExchange): RelNode = visit(exchange.asInstanceOf[RelNode])

  override def visit(scan: TableScan): RelNode = visit(scan.asInstanceOf[RelNode])

  override def visit(scan: TableFunctionScan): RelNode = visit(scan.asInstanceOf[RelNode])

  override def visit(values: LogicalValues): RelNode = visit(values.asInstanceOf[RelNode])

  override def visit(filter: LogicalFilter): RelNode = visit(filter.asInstanceOf[RelNode])

  override def visit(project: LogicalProject): RelNode = visit(project.asInstanceOf[RelNode])

  override def visit(join: LogicalJoin): RelNode = visit(join.asInstanceOf[RelNode])

  override def visit(correlate: LogicalCorrelate): RelNode = visit(correlate.asInstanceOf[RelNode])
}

/**
  * Convert logical table scan to a relational expression.
  */
class ExpandTableScanShuttle extends RelShuttleImpl {

  /**
    * Override this method to use `replaceInput` method instead of `copy` method
    * if any children change. This will not change any output of LogicalTableScan
    * when LogicalTableScan is replaced with RelNode tree in its RelTable.
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

  /**
    * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling [[RelTable]]#toRel
    */
  override def visit(scan: TableScan): RelNode = {
    scan match {
      case tableScan: LogicalTableScan =>
        val relTable = tableScan.getTable.unwrap(classOf[RelTable])
        if (relTable != null) {
          val rel = relTable.toRel(RelOptUtil.getContext(tableScan.getCluster), tableScan.getTable)
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
