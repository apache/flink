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

package org.apache.flink.table.planner.plan.rules.logical

import java.util.Collections

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._
import org.apache.flink.table.functions.FunctionIdentifier
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.functions.SqlUnnestUtils

/**
  * Planner rule that rewrites UNNEST to explode function.
  *
  * Note: This class can only be used in HepPlanner.
  */
class LogicalUnnestRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalCorrelate = call.rel(0)
    val right = getRel(join.getRight)

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        getRel(filter.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case p: LogicalProject => getRel(p.getInput) match {
            case u: Uncollect => !u.withOrdinality
            case _ => false
          }
          case _ => false
        }
      case project: LogicalProject =>
        getRel(project.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case _ => false
        }
      case u: Uncollect => !u.withOrdinality
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val outer = getRel(correlate.getLeft)
    val array = getRel(correlate.getRight)

    def convert(relNode: RelNode): RelNode = {
      relNode match {
        case rs: HepRelVertex =>
          convert(getRel(rs))

        case f: LogicalProject =>
          f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput))))

        case f: LogicalFilter =>
          f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput))))

        case uc: Uncollect =>
          // convert Uncollect into TableFunctionScan
          val cluster = correlate.getCluster
          val typeFactory = ShortcutUtils.unwrapTypeFactory(cluster)
          val relDataType = uc.getInput.getRowType.getFieldList.get(0).getValue
          val logicalType = FlinkTypeFactory.toLogicalType(relDataType)

          val unnestFunction = SqlUnnestUtils.createUnnestFunction(logicalType)

          val sqlFunction = BridgingSqlFunction.of(
            cluster,
            FunctionIdentifier.of("UNNEST"),
            unnestFunction)

          val rexCall = cluster.getRexBuilder.makeCall(
            typeFactory.createFieldTypeFromLogicalType(unnestFunction.getWrappedOutputType),
            sqlFunction,
            getRel(uc.getInput).asInstanceOf[LogicalProject].getChildExps)

          new LogicalTableFunctionScan(
            cluster,
            correlate.getTraitSet,
            Collections.emptyList(),
            rexCall,
            null,
            rexCall.getType,
            null)
      }
    }

    // convert unnest into table function scan
    val tableFunctionScan = convert(array)
    // create correlate with table function scan as input
    val newCorrelate =
      correlate.copy(correlate.getTraitSet, ImmutableList.of(outer, tableFunctionScan))
    call.transformTo(newCorrelate)
  }

  private def getRel(rel: RelNode): RelNode = {
    rel match {
      case vertex: HepRelVertex => vertex.getCurrentRel
      case _ => rel
    }
  }
}

object LogicalUnnestRule {
  val INSTANCE = new LogicalUnnestRule(
    operand(classOf[LogicalCorrelate], any),
    "LogicalUnnestRule")
}
