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

import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}

import scala.collection.JavaConversions._

class DefaultRelShuttle extends RelShuttle {

  override def visit(rel: RelNode): RelNode = {
    rel.getInputs.zipWithIndex.foreach {
      case (input, index) =>
        val newInput = input.accept(this)
        if (input ne newInput) {
          rel.replaceInput(index, newInput)
        }
    }
    rel
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
