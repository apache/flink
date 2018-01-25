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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexProgram, RexProgramBuilder}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableFunctionScan}

/**
  * An utility for datasteam and dataset correlate rules.
  */
object CorrelateUtil {

  /**
    * Find only calc and table function
    */
  def findCalcAndTableFunction(calc: FlinkLogicalCalc): Boolean = {
    val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
    child match {
      case scan: FlinkLogicalTableFunctionScan => true
      case calc: FlinkLogicalCalc => findCalcAndTableFunction(calc)
      case _ => false
    }
  }

  /**
    * Get [[FlinkLogicalTableFunctionScan]] from the input calc.
    */
  def getTableScan(calc: FlinkLogicalCalc): RelNode = {
    val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
    child match {
      case scan: FlinkLogicalTableFunctionScan => scan
      case calc: FlinkLogicalCalc => getTableScan(calc)
      case _ => throw TableException("This must be a bug, could not find table scan")
    }
  }

  /**
    * Merge continuous calcs.
    *
    * @param calc the input calc
    * @return the sinle merged calc
    */
  def getMergedCalc(calc: FlinkLogicalCalc): FlinkLogicalCalc = {
    val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
    if (child.isInstanceOf[FlinkLogicalCalc]) {
      val bottomCalc = getMergedCalc(child.asInstanceOf[FlinkLogicalCalc])
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
    } else {
      calc
    }
  }
}
