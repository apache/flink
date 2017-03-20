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

import java.util

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.nodes.TableSourceScan
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.util.RexProgramExtractor
import org.apache.flink.table.sources.FilterableTableSource
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

trait PushFilterIntoTableSourceScanRuleBase {

  private[flink] def pushFilterIntoScan(
      call: RelOptRuleCall,
      calc: Calc,
      scan: TableSourceScan,
      tableSourceTable: TableSourceTable[_],
      filterableSource: FilterableTableSource[_],
      description: String): Unit = {

    Preconditions.checkArgument(!filterableSource.isFilterPushedDown)

    val program = calc.getProgram
    val functionCatalog = FunctionCatalog.withBuiltIns
    val (predicates, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        call.builder().getRexBuilder,
        functionCatalog)
    if (predicates.isEmpty) {
      // no condition can be translated to expression
      return
    }

    val remainingPredicates = new util.LinkedList[Expression]()
    predicates.foreach(e => remainingPredicates.add(e))

    val newTableSource = filterableSource.applyPredicate(remainingPredicates)

    // check whether framework still need to do a filter
    val relBuilder = call.builder()
    val remainingCondition = {
      if (!remainingPredicates.isEmpty || unconvertedRexNodes.nonEmpty) {
        relBuilder.push(scan)
        val remainingConditions =
          (remainingPredicates.asScala.map(expr => expr.toRexNode(relBuilder))
              ++ unconvertedRexNodes)
        remainingConditions.reduce((l, r) => relBuilder.and(l, r))
      } else {
        null
      }
    }

    // check whether we still need a RexProgram. An RexProgram is needed when either
    // projection or filter exists.
    val newScan = scan.copy(scan.getTraitSet, newTableSource)
    val newRexProgram = {
      if (remainingCondition != null || !program.projectsOnlyIdentity) {
        val expandedProjectList = program.getProjectList.asScala
            .map(ref => program.expandLocalRef(ref)).asJava
        RexProgram.create(
          program.getInputRowType,
          expandedProjectList,
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
