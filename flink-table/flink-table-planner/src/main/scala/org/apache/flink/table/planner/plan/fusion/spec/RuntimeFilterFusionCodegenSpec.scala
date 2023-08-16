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
package org.apache.flink.table.planner.plan.fusion.spec

import org.apache.flink.runtime.operators.util.BloomFilter
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, newNames}
import org.apache.flink.table.planner.plan.fusion.{OpFusionCodegenSpecBase, OpFusionContext}
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions

import java.util

/** The operator fusion codegen spec for RuntimeFilter. */
class RuntimeFilterFusionCodegenSpec(opCodegenCtx: CodeGeneratorContext, probeIndices: Array[Int])
  extends OpFusionCodegenSpecBase(opCodegenCtx) {

  private lazy val buildInputId = 1

  private var buildContext: OpFusionContext = _
  private var probeContext: OpFusionContext = _
  private var buildType: RowType = _
  private var probeType: RowType = _

  private var buildComplete: String = _
  private var filterTerm: String = _

  override def setup(opFusionContext: OpFusionContext): Unit = {
    super.setup(opFusionContext)
    val inputContexts = fusionContext.getInputFusionContexts
    assert(inputContexts.size == 2)
    buildContext = inputContexts.get(0)
    probeContext = inputContexts.get(1)

    buildType = buildContext.getOutputType
    probeType = probeContext.getOutputType
  }

  override def variablePrefix(): String = "runtimeFilter"

  override def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildContext.processProduce(codegenCtx)
    probeContext.processProduce(codegenCtx)
  }

  override def doEndInputProduce(codegenCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildContext.endInputProduce(codegenCtx)
    probeContext.endInputProduce(codegenCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    if (inputId == buildInputId) {
      buildComplete = newName("buildComplete")
      opCodegenCtx.addReusableMember(s"private transient boolean $buildComplete;")
      opCodegenCtx.addReusableOpenStatement(s"$buildComplete = false;")

      filterTerm = newName("filter")
      val filterClass = className[BloomFilter]
      opCodegenCtx.addReusableMember(s"private transient $filterClass $filterTerm;")

      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |if ($filterTerm == null && !${row.resultTerm}.isNullAt(1)) {
         |    $filterTerm = $filterClass.fromBytes(${row.resultTerm}.getBinary(1));
         |}
         |""".stripMargin
    } else {
      val Seq(probeKeyTerm, probeKeyWriterTerm) = newNames("probeKeyTerm", "probeKeyWriterTerm")
      // project probe key row from input
      val probeKeyExprs = probeIndices.map(idx => inputVars.get(idx))
      val keyProjectionCode = getExprCodeGenerator
        .generateResultExpression(
          probeKeyExprs,
          RowTypeUtils.projectRowType(probeType, probeIndices),
          classOf[BinaryRowData],
          probeKeyTerm,
          outRowWriter = Option(probeKeyWriterTerm))
        .code

      val found = newName("found")
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |
         |boolean $found = true;
         |if ($filterTerm != null) {
         |  // compute the hash code of probe key
         |  $keyProjectionCode
         |  final int hashCode = $probeKeyTerm.hashCode();
         |  if (!$filterTerm.testHash(hashCode)) {
         |    $found = false;
         |  }
         |}
         |// if found, call downstream to consume the row
         |if($found) {
         |  ${row.code}
         |  ${fusionContext.processConsume(null, row.resultTerm)}
         |}
         |""".stripMargin
    }
  }

  override def doEndInputConsume(inputId: Int): String = {
    if (inputId == buildInputId) {
      s"""
         |${className[Preconditions]}.checkState(!$buildComplete, "Should not build completed.");
         |LOG.info("RuntimeFilter build completed.");
         |$buildComplete = true;
         |""".stripMargin
    } else {
      s"""
         |${className[Preconditions]}.checkState($buildComplete, "Should build completed.");
         |LOG.info("Finish RuntimeFilter probe phase.");
         |// call downstream endInput method
         |${fusionContext.endInputConsume()}
         |""".stripMargin
    }
  }
}
