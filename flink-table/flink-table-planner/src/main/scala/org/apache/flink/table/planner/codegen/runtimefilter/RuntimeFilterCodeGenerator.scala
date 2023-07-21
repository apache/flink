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
package org.apache.flink.table.planner.codegen.runtimefilter

import org.apache.flink.runtime.operators.util.BloomFilter
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, OperatorCodeGenerator, ProjectionCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, DEFAULT_INPUT1_TERM, DEFAULT_INPUT2_TERM, ROW_DATA}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{generateCollect, INPUT_SELECTION}
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions

/** Operator code generator for runtime filter operator. */
object RuntimeFilterCodeGenerator {
  def gen(
      ctx: CodeGeneratorContext,
      buildType: RowType,
      probeType: RowType,
      probeIndices: Array[Int]): CodeGenOperatorFactory[RowData] = {
    val probeGenProj = ProjectionCodeGenerator.generateProjection(
      ctx,
      "RuntimeFilterProjection",
      probeType,
      RowTypeUtils.projectRowType(probeType, probeIndices),
      probeIndices)
    ctx.addReusableInnerClass(probeGenProj.getClassName, probeGenProj.getCode)

    val probeProjection = newName("probeToBinaryRow")
    ctx.addReusableMember(s"private transient ${probeGenProj.getClassName} $probeProjection;")
    val probeProjRefs = ctx.addReusableObject(probeGenProj.getReferences, "probeProjRefs")
    ctx.addReusableOpenStatement(
      s"$probeProjection = new ${probeGenProj.getClassName}($probeProjRefs);")

    val buildComplete = newName("buildComplete")
    ctx.addReusableMember(s"private transient boolean $buildComplete;")
    ctx.addReusableOpenStatement(s"$buildComplete = false;")

    val filter = newName("filter")
    val filterClass = className[BloomFilter]
    ctx.addReusableMember(s"private transient $filterClass $filter;")

    val processElement1Code =
      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |
         |if ($filter == null && !$DEFAULT_INPUT1_TERM.isNullAt(1)) {
         |    $filter = $filterClass.fromBytes($DEFAULT_INPUT1_TERM.getBinary(1));
         |}
         |""".stripMargin

    val processElement2Code =
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |
         |if ($filter != null) {
         |    final int hashCode = $probeProjection.apply($DEFAULT_INPUT2_TERM).hashCode();
         |    if ($filter.testHash(hashCode)) {
         |        ${generateCollect(s"$DEFAULT_INPUT2_TERM")}
         |    }
         |} else {
         |    ${generateCollect(s"$DEFAULT_INPUT2_TERM")}
         |}
         |""".stripMargin

    val nextSelectionCode =
      s"return $buildComplete ? $INPUT_SELECTION.SECOND : $INPUT_SELECTION.FIRST;"

    val endInputCode1 =
      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |
         |LOG.info("RuntimeFilter build completed.");
         |$buildComplete = true;
         |""".stripMargin

    val endInputCode2 =
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |
         |LOG.info("Finish RuntimeFilter probe phase.");
         |""".stripMargin

    new CodeGenOperatorFactory[RowData](
      OperatorCodeGenerator.generateTwoInputStreamOperator(
        ctx,
        "RuntimeFilterOperator",
        processElement1Code,
        processElement2Code,
        buildType,
        probeType,
        DEFAULT_INPUT1_TERM,
        DEFAULT_INPUT2_TERM,
        nextSelectionCode = Some(nextSelectionCode),
        endInputCode1 = Some(endInputCode1),
        endInputCode2 = Some(endInputCode2)
      ))
  }
}
