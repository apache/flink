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
import org.apache.calcite.rel.core.Calc
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.nodes.TableSourceScan
import org.apache.flink.table.plan.util.{RexProgramExtractor, RexProgramRewriter}
import org.apache.flink.table.sources.{NestedFieldsProjectableTableSource, ProjectableTableSource}

trait PushProjectIntoTableSourceScanRuleBase {

  private[flink] def pushProjectIntoScan(
      call: RelOptRuleCall,
      calc: Calc,
      scan: TableSourceScan): Unit = {

    val usedFields = RexProgramExtractor.extractRefInputFields(calc.getProgram)

    // if no fields can be projected, we keep the original plan.
    val source = scan.tableSource
    if (TableEnvironment.getFieldNames(source).length != usedFields.length) {

      val newTableSource = source match {
        case nested: NestedFieldsProjectableTableSource[_] =>
          val nestedFields = RexProgramExtractor
            .extractRefNestedInputFields(calc.getProgram, usedFields)
          nested.projectNestedFields(usedFields, nestedFields)
        case projecting: ProjectableTableSource[_] =>
          projecting.projectFields(usedFields)
      }

      val newScan = scan.copy(scan.getTraitSet, newTableSource)
      val newCalcProgram = RexProgramRewriter.rewriteWithFieldProjection(
        calc.getProgram,
        newScan.getRowType,
        calc.getCluster.getRexBuilder,
        usedFields)

      if (newCalcProgram.isTrivial) {
        // drop calc if the transformed program merely returns its input and doesn't exist filter
        call.transformTo(newScan)
      } else {
        val newCalc = calc.copy(calc.getTraitSet, newScan, newCalcProgram)
        call.transformTo(newCalc)
      }
    }
  }
}
