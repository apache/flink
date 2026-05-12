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
package org.apache.flink.table.planner.plan.rules.physical.common

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexNode, RexProgram, RexProgramBuilder, RexShuttle}

import scala.collection.JavaConverters._

/**
 * Shared helpers for the physical Correlate rules that need to preserve a right-side projection
 * (originally on a {@link org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc}
 * between the Correlate and the {@link
 * org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan}) by applying it
 * via a wrapping Calc on top of the physical Correlate.
 */
object CorrelateProjectionUtils {

  /**
   * Builds a {@link RexProgram} that re-applies the Calc's original projection to the output of the
   * physical Correlate (left ++ scan.rowType). Left fields are passed through unchanged; right-side
   * projection expressions have their input references shifted by {@code leftFieldCount} so they
   * index the combined input row.
   */
  def buildWrappingProgram(
      originalProgram: RexProgram,
      combinedRowType: RelDataType,
      outputRowType: RelDataType,
      leftFieldCount: Int,
      rexBuilder: RexBuilder): RexProgram = {

    val shifter = new RexShuttle {
      override def visitInputRef(inputRef: RexInputRef): RexNode =
        new RexInputRef(inputRef.getIndex + leftFieldCount, inputRef.getType)
    }

    val builder = new RexProgramBuilder(combinedRowType, rexBuilder)

    // Pass through the left fields by name and type from the combined row.
    val combinedFields = combinedRowType.getFieldList
    for (i <- 0 until leftFieldCount) {
      val field = combinedFields.get(i)
      builder.addProject(new RexInputRef(i, field.getType), field.getName)
    }

    // Append the original Calc's projection list, with input refs shifted to point at the
    // right-hand portion of the combined row, and rename to match outputRowType.
    val originalProjects = originalProgram.getProjectList.asScala
    val outputFields = outputRowType.getFieldList
    require(
      leftFieldCount + originalProjects.size == outputFields.size,
      s"output field count ${outputFields.size} != left ($leftFieldCount) + projection " +
        s"(${originalProjects.size})"
    )
    for (i <- originalProjects.indices) {
      val expanded = originalProgram.expandLocalRef(originalProjects(i)).accept(shifter)
      val outputName = outputFields.get(leftFieldCount + i).getName
      builder.addProject(expanded, outputName)
    }

    builder.getProgram
  }
}
