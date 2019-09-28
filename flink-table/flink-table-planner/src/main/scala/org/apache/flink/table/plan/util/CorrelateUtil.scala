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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rex.{RexProgram, RexProgramBuilder}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableFunctionScan}

/**
  * A utility for datastream and dataset correlate rules.
  */
object CorrelateUtil {

  /**
    * Get [[FlinkLogicalTableFunctionScan]] from the input calc. Returns None if there is no table
    * function at the end.
    */
  def getTableFunctionScan(calc: FlinkLogicalCalc): Option[FlinkLogicalTableFunctionScan] = {
    val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
    child match {
      case scan: FlinkLogicalTableFunctionScan => Some(scan)
      case calc: FlinkLogicalCalc => getTableFunctionScan(calc)
      case _ => None
    }
  }

  /**
    * Merge continuous calcs.
    *
    * @param calc the input calc
    * @return the single merged calc
    */
  def getMergedCalc(calc: FlinkLogicalCalc): FlinkLogicalCalc = {
    val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
    child match {
      case logicalCalc: FlinkLogicalCalc =>
        val bottomCalc = getMergedCalc(logicalCalc)
        val topCalc = calc
        val topProgram: RexProgram = topCalc.getProgram
        val mergedProgram: RexProgram = RexProgramBuilder
          .mergePrograms(
            topCalc.getProgram,
            bottomCalc.getProgram,
            topCalc.getCluster.getRexBuilder)
        assert(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)
        topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
          .asInstanceOf[FlinkLogicalCalc]
      case _ =>
        calc
    }
  }
}
