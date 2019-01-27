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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.util.CalcUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata.{RelMdCollation, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexProgram}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConversions._

class FlinkLogicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram)
  extends Calc(cluster, traitSet, input, calcProgram)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new FlinkLogicalCalc(cluster, traitSet, child, program)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    FlinkLogicalCalc.computeCost(calcProgram, planner, mq, this)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
        .item("select", CalcUtil.selectionToString(calcProgram, getExpressionString))
        .itemIf(
          "where",
          CalcUtil.conditionToString(calcProgram, getExpressionString),
          calcProgram.getCondition != null)
  }

  override def isDeterministic: Boolean = CalcUtil.isDeterministic(program)
}

private class FlinkLogicalCalcConverter
  extends ConverterRule(
    classOf[LogicalCalc],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCalcConverter") {

  override def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[LogicalCalc]
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalCalc.create(newInput, calc.getProgram)
  }
}

object FlinkLogicalCalc {

  val CONVERTER: ConverterRule = new FlinkLogicalCalcConverter()

  def create(
      input: RelNode,
      calcProgram: RexProgram): FlinkLogicalCalc = {
    val cluster = input.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSet.replace(Convention.NONE).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, calcProgram)
      })
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalCalc to
    // calculate the distribution trait
    val calc = new FlinkLogicalCalc(cluster, traitSet, input, calcProgram)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(calc)
      .replace(FlinkConventions.LOGICAL).simplify()
    calc.copy(newTraitSet, calc.getInputs).asInstanceOf[FlinkLogicalCalc]
  }

  def computeCost(
      calcProgram: RexProgram,
      planner: RelOptPlanner,
      mq: RelMetadataQuery,
      calc: Calc): RelOptCost = {
    // compute number of expressions that do not access a field or literal, i.e. computations,
    // conditions, etc. We only want to account for computations, not for simple projections.
    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
    // in normalization stage. So we should ignore CASTs here in optimization stage.
    val compCnt = calcProgram.getProjectList.map(calcProgram.expandLocalRef).toList.count {
      case _: RexInputRef => false
      case _: RexLiteral => false
      case c: RexCall if c.getOperator.getName.equals("CAST") => false
      case _ => true
    }
    val newRowCnt = mq.getRowCount(calc)
    planner.getCostFactory.makeCost(newRowCnt, newRowCnt * compCnt, 0)
  }
}
