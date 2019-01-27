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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.plan.FlinkJoinRelType._
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecExchange, BatchExecHashJoinBase}
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule._
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexProgramBuilder
import org.apache.calcite.util.ImmutableBitSet
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

/**
  * Insert the runtime filter builder to build side and insert the runtime filter node to
  * the probe side.
  */
class InsertRuntimeFilterRule
    extends RelOptRule(
      operand(classOf[BatchExecHashJoinBase], operand(classOf[RelNode], any)),
      "InsertRuntimeFilterRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: BatchExecHashJoinBase = call.rel(0)

    val conf = FlinkRelOptUtil.getTableConfig(join)

    val enableRuntimeFilter = conf.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED)

    enableRuntimeFilter &&
        !join.isBroadcast && // now not support broadcast join.
        (join.flinkJoinType == INNER || join.flinkJoinType == SEMI) &&
        !join.haveInsertRf
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: BatchExecHashJoinBase = call.rel(0)

    join.insertRuntimeFilter()

    val relBuilder = call.builder()
    val rexBuilder = join.getCluster.getRexBuilder

    def getNonExchangeInput(rel: RelNode): RelNode = {
      getHepRel(rel) match {
        case exchange: BatchExecExchange => exchange.getInput
        case p => p
      }
    }

    // get real input.
    val buildInput = getNonExchangeInput(join.buildRel)
    val probeInput = getNonExchangeInput(join.probeRel)

    // build RexProgram
    val buildRexPBuilder = new RexProgramBuilder(buildInput.getRowType, rexBuilder)
    val probeRexPBuilder = new RexProgramBuilder(probeInput.getRowType, rexBuilder)

    var filterCount = 0
    join.buildKeys.zipWithIndex.foreach {case (buildKey, i) =>

      val probeKey = join.probeKeys(i)

      val mq = join.getCluster.getMetadataQuery
      val buildKeyNdv = mq.getDistinctRowCount(join.buildRel, ImmutableBitSet.of(buildKey), null)
      val probeKeyNdv = mq.getDistinctRowCount(join.probeRel, ImmutableBitSet.of(probeKey), null)
      val buildRowCount = mq.getRowCount(join.buildRel)
      val probeRowCount = mq.getRowCount(join.probeRel)

      if (buildKeyNdv != null &&
          probeKeyNdv != null &&
          buildRowCount != null &&
          probeRowCount != null) {
        val broadcastId = String.valueOf(
          InsertRuntimeFilterRule.BROADCAST_ID_COUNTER.getAndIncrement)
        val buildSqlFunc = new SqlRuntimeFilterBuilderFunction(
          broadcastId,
          buildKeyNdv,
          buildRowCount)
        buildRexPBuilder.addCondition(
          relBuilder.call(buildSqlFunc, buildRexPBuilder.makeInputRef(buildKey)))

        val probeSqlFunc = new SqlRuntimeFilterFunction(
          probeKeyNdv,
          probeRowCount,
          buildSqlFunc)
        buildSqlFunc.filters += probeSqlFunc
        probeRexPBuilder.addCondition(
          relBuilder.call(probeSqlFunc, probeRexPBuilder.makeInputRef(probeKey)))

        filterCount = filterCount + 1
      }
    }

    if (filterCount == 0) {
      return
    }

    var inputs = addFilterToJoinInputs(join.getInputs, join.buildRel, buildInput, buildRexPBuilder)
    inputs = addFilterToJoinInputs(inputs, join.probeRel, probeInput, probeRexPBuilder)
    call.transformTo(join.copy(join.getTraitSet, inputs))
  }

  def addFilterToJoinInputs(
      allJoinInputs: Seq[RelNode],
      joinInput: RelNode,
      filterInput: RelNode,
      pgBuilder: RexProgramBuilder): Seq[RelNode] = {
    projectAllFields(filterInput, pgBuilder)
    val program = pgBuilder.getProgram
    val filter = new BatchExecCalc(
      filterInput.getCluster,
      filterInput.getTraitSet,
      filterInput,
      filterInput.getRowType,
      program,
      "HashJoinRuntimeFilter")
    val (oldRel, newRel) = getHepRel(joinInput) match {
      case exchange: BatchExecExchange =>
        (exchange, exchange.copy(exchange.getTraitSet, Collections.singletonList(filter)))
      case p => (p, filter)
    }
    allJoinInputs.map((rel) => if (getHepRel(rel) eq oldRel) newRel else rel)
  }
}

object InsertRuntimeFilterRule {

  val INSTANCE = new InsertRuntimeFilterRule

  val BROADCAST_ID_COUNTER = new AtomicInteger(0)

  def resetBroadcastIdCounter(): Unit = {
    BROADCAST_ID_COUNTER.set(0)
  }

  def getHepRel(rel: RelNode): RelNode = rel match {
    case hepRel: HepRelVertex => hepRel.getCurrentRel
    case _ => rel
  }

  def projectAllFields(input: RelNode, programBuilder: RexProgramBuilder): Unit = {
    input.getRowType.getFieldNames.zipWithIndex.foreach {
      case (name, i) => programBuilder.addProject(i, name)
    }
  }
}
