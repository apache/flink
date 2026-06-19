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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.plan.fusion.FusionCodegenUtil.{evaluateRequiredVariables, extractRefInputFields}
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecBase
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}

import org.apache.calcite.rex.{RexInputRef, RexNode}

import java.util

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/** The operator fusion codegen spec for Calc. */
class CalcFusionCodegenSpec(
    opCodegenCtx: CodeGeneratorContext,
    projection: Seq[RexNode],
    condition: Option[RexNode])
  extends OpFusionCodegenSpecBase(opCodegenCtx) {

  override def variablePrefix: String = "calc"

  override def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit = {
    assert(fusionContext.getInputFusionContexts.size == 1)
    fusionContext.getInputFusionContexts.head.processProduce(codegenCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    val onlyFilter =
      projection.lengthCompare(
        fusionContext.getInputFusionContexts.head.getOutputType.getFieldCount) == 0 &&
        projection.zipWithIndex.forall {
          case (rexNode, index) =>
            rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
        }

    val projectionUsedColumns =
      extractRefInputFields(projection, fusionContext.getInputFusionContexts.head.getOutputType)
    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionExprs = projection.map(getExprCodeGenerator.generateExpression)
      s"""
         |${opCodegenCtx.reuseLocalVariableCode()}
         |${evaluateRequiredVariables(toScala(inputVars), projectionUsedColumns)}
         |${fusionContext.processConsume(toJava(projectionExprs))}
         |""".stripMargin
    } else {
      // materialize the field access code in advance which is referenced by filter condition to avoid it be evaluated more time
      val filterUsedColumns = extractRefInputFields(
        Seq(condition.get),
        fusionContext.getInputFusionContexts.head.getOutputType)
      val filterAccessCode = evaluateRequiredVariables(toScala(inputVars), filterUsedColumns)
      val filterExpr = getExprCodeGenerator.generateExpression(condition.get)
      // only filter
      if (onlyFilter) {
        s"""
           |$filterAccessCode
           |${opCodegenCtx.reuseLocalVariableCode()}
           |${filterExpr.code}
           |if (${filterExpr.resultTerm}) {
           |  ${fusionContext.processConsume(inputVars)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        // if any filter conditions, projection code will enter an new scope
        val projectionExprs = projection.map(getExprCodeGenerator.generateExpression)
        s"""
           |$filterAccessCode
           |${opCodegenCtx.reuseLocalVariableCode()}
           |${filterExpr.code}
           |if (${filterExpr.resultTerm}) {
           |  ${evaluateRequiredVariables(toScala(inputVars), projectionUsedColumns)}
           |  ${fusionContext.processConsume(toJava(projectionExprs))}
           |}
           |""".stripMargin
      }
    }
  }

  override def doEndInputProduce(codegenCtx: CodeGeneratorContext): Unit = {
    fusionContext.getInputFusionContexts.head.endInputProduce(codegenCtx)
  }

  override def doEndInputConsume(inputId: Int): String = {
    // Nothing need to do for calc in endInput, just propagate to downstream
    fusionContext.endInputConsume()
  }
}
