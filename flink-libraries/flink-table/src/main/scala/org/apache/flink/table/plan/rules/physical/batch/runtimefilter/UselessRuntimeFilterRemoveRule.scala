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

package org.apache.flink.table.plan.rules.physical.batch.runtimefilter

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRuntimeFilters
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.UselessRuntimeFilterRemoveRule._
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.util.BloomFilter

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexCall, RexProgramBuilder}
import org.apache.calcite.sql.SqlOperator

import java.lang

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that removes a useless [[SqlRuntimeFilterFunction]] (Not worth doing).
  */
class UselessRuntimeFilterRemoveRule extends RelOptRule(
  operand(classOf[BatchExecCalc], operand(classOf[RelNode], any)),
  "UselessRuntimeFilterRemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRuntimeFilters(calc.getProgram).nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = call.rel(0)
    val conf = FlinkRelOptUtil.getTableConfig(calc)

    val minProbeRowCount = conf.getConf.getLong(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN)

    val maxRowCountRatio = conf.getConf.getDouble(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX)

    val rfs = findRuntimeFilters(calc.getProgram)
        .map(_.getOperator.asInstanceOf[SqlRuntimeFilterFunction])
    val toBeRemove = new mutable.ArrayBuffer[SqlOperator]
    rfs.foreach { rf =>
      val suitable = ndvRowCountSuitable(conf, rf.rowCount, rf.builder.ndv, rf.ndv) &&
          rf.rowCount >= minProbeRowCount &&
          rf.builder.rowCount / rf.rowCount <= maxRowCountRatio
      if (!suitable) {
        rf.builder.filters -= rf
        toBeRemove += rf
      }
    }

    if (toBeRemove.nonEmpty) {
      call.transformTo(removeFilters(calc, toBeRemove.toArray))
    }
  }
}

object UselessRuntimeFilterRemoveRule {

  val INSTANCE = new UselessRuntimeFilterRemoveRule

  /**
    * We estimate an FPP and see how much of the probe's data can be filtered out
    * based on this FPP.
    */
  private def ndvRowCountSuitable(
      conf: TableConfig,
      probeRowCount: lang.Double,
      buildNdv: lang.Double,
      probeNdv: lang.Double) = {

    val minProbeFilter = conf.getConf.getDouble(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN)

    val minFpp = getMinSuitableFpp(conf, probeRowCount, buildNdv)

    (1 - buildNdv / probeNdv) * (1 - minFpp) >= minProbeFilter
  }

  /**
    * Estimate the FPP based on BloomFilter's probability and probe row count.
    */
  def getMinSuitableFpp(
      conf: TableConfig,
      probeRowCount: lang.Double,
      buildNdv: lang.Double): Double = {

    val ratioOfRowAndBits = conf.getConf.getInteger(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO)

    val confMaxNumOfBits = conf.getConf.getInteger(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_SIZE_MAX) * 1024L * 1024L * 8L

    val maxNumOfBits = Math.min(probeRowCount / ratioOfRowAndBits, confMaxNumOfBits)

    BloomFilter.findSuitableFpp(buildNdv.longValue(), maxNumOfBits)
  }

  /**
    * Remove specific SqlOperators to calc.
    */
  def removeFilters(calc: BatchExecCalc, toBeRemove: Array[SqlOperator]): Calc = {
    val rexBuilder = calc.getCluster.getRexBuilder
    val program = calc.getProgram

    val pBuilder = RexProgramBuilder.forProgram(program, rexBuilder, true)
    pBuilder.clearCondition()

    val filters = RelOptUtil.conjunctions(program.expandLocalRef(program.getCondition))
    filters.filter {
      case call: RexCall => !toBeRemove.contains(call.getOperator)
      case _ => true
    }.foreach(pBuilder.addCondition)

    calc.copy(calc.getTraitSet, calc.getInput, pBuilder.getProgram())
  }
}
