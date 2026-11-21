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

/** Shared helpers for the physical Correlate rules. */
object CorrelateProjectionUtils {

  /**
   * Re-applies a Calc's projection above the physical Correlate. Left fields are passed through;
   * right-side input refs are shifted by {@code leftFieldCount} to index the combined row.
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

    val combinedFields = combinedRowType.getFieldList
    for (i <- 0 until leftFieldCount) {
      val field = combinedFields.get(i)
      builder.addProject(new RexInputRef(i, field.getType), field.getName)
    }

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
