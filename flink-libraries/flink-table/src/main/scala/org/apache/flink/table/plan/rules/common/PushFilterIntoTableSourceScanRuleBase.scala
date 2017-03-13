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

package org.apache.flink.table.plan.rules.common

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.{Calc, TableScan}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.table.plan.nodes.dataset.DataSetCalc
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.util.RexProgramExtractor
import org.apache.flink.table.sources.FilterableTableSource

trait PushFilterIntoTableSourceScanRuleBase {

  private[flink] def pushFilterIntoScan(
      call: RelOptRuleCall,
      calc: Calc,
      scan: TableScan,
      tableSourceTable: TableSourceTable[_],
      filterableSource: FilterableTableSource,
      description: String): Unit = {

    if (filterableSource.isFilterPushedDown) {
      // The rule can get triggered again due to the transformed "scan => filter"
      // sequence created by the earlier execution of this rule when we could not
      // push all the conditions into the scan
      return
    }

    val program = calc.getProgram
    val (predicates, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        call.builder().getRexBuilder,
        tableSourceTable.tableEnv.getFunctionCatalog)
    if (predicates.isEmpty) {
      // no condition can be translated to expression
      return
    }

    // trying to apply filter push down, set the flag to true no matter whether
    // we actually push any filters down.
    filterableSource.setFilterPushedDown(true)
    val remainingPredicates = filterableSource.applyPredicate(predicates)

    // check whether framework still need to do a filter
    val relBuilder = call.builder()
    val remainingCondition = {
      if (remainingPredicates.length > 0 || unconvertedRexNodes.length > 0) {
        relBuilder.push(scan)
        (remainingPredicates.map(expr => expr.toRexNode(relBuilder)) ++ unconvertedRexNodes)
            .reduce((l, r) => relBuilder.and(l, r))
      } else {
        null
      }
    }

    // check whether we still need a RexProgram. An RexProgram is needed when either
    // projection or filter exists.
    val newScan = scan.copy(scan.getTraitSet, null)
    val newRexProgram = {
      if (remainingCondition != null || program.getProjectList.size() > 0) {
        RexProgram.create(
          program.getInputRowType,
          program.getProjectList,
          remainingCondition,
          program.getOutputRowType,
          relBuilder.getRexBuilder)
      } else {
        null
      }
    }

    if (newRexProgram != null) {
      val newCalc = calc.copy(calc.getTraitSet, newScan, newRexProgram)
      call.transformTo(newCalc)
    } else {
      call.transformTo(newScan)
    }
  }
}
