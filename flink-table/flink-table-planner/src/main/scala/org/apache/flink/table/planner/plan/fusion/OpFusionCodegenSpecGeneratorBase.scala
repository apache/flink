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
package org.apache.flink.table.planner.plan.fusion

import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression, GenerateUtils}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{fieldIndices, newName, DEFAULT_OUT_RECORD_WRITER_TERM}
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.types.logical.RowType

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer

/** The base class of {@link OpFusionCodegenSpecGenerator} that support operator fusion codegen. */
abstract class OpFusionCodegenSpecGeneratorBase(
    managedMemory: Long,
    outputType: RowType,
    fusionCodegenSpec: OpFusionCodegenSpec)
  extends OpFusionCodegenSpecGenerator(outputType, fusionCodegenSpec) {

  /**
   * If an operator has multiple outputs in the OpFusionCodegenSpec DAG, we only need to call it
   * produce method once, so we need these two flag variable.
   */
  private var hasProcessProduceTraversed = false
  private var hasEndInputProduceTraversed = false

  private lazy val outputRowTerm: String = newName(variablePrefix + "outputRow")

  private val outputs: ListBuffer[(Int, OpFusionCodegenSpecGenerator)] =
    ListBuffer[(Int, OpFusionCodegenSpecGenerator)]()

  def addOutput(inputIdOfOutput: Int, output: OpFusionCodegenSpecGenerator): Unit = {
    outputs += ((inputIdOfOutput, output))
  }

  override def getManagedMemory: Long = managedMemory

  def processProduce(fusionCtx: CodeGeneratorContext): Unit = {
    if (!hasProcessProduceTraversed) {
      opFusionCodegenSpec.doProcessProduce(fusionCtx)
      hasProcessProduceTraversed = true
    }
  }

  def processConsume(
      outputVars: java.util.List[GeneratedExpression],
      row: String = null): String = {
    val (inputVars, localVariables) = if (outputVars != null) {
      assert(outputVars.size() == outputType.getFieldCount)
      (toScala(outputVars), "")
    } else {
      assert(row != null, "outputVars and row can't both be null.")
      getOperatorCtx.startNewLocalVariableStatement(row)
      val fieldExprs = fieldIndices(outputType)
        .map(index => GenerateUtils.generateFieldAccess(getOperatorCtx, outputType, row, index))
        .toSeq
      (fieldExprs, getOperatorCtx.reuseLocalVariableCode(row))
    }

    // if this operator has multiple output operators, we need to materialize all vars in advance to
    // avoid be evaluated multiple times in downstream
    val evaluatedAllVars = if (outputs.length > 1) {
      toScala(outputVars).map(expr => expr.getCode).mkString("\n")
    } else {
      ""
    }

    // iterate each output separately
    val consumeCode = outputs
      .map(
        op => {
          val inputIdOfOutput = op._1
          val outputSpec = op._2.getOpFusionCodegenSpec

          // evaluate the expr code which will be used more than once in advance to avoid evaluated more time
          val evaluatedReqVars =
            evaluateRequiredVariables(inputVars, outputSpec.usedInputs(inputIdOfOutput))
          val inputRowDataClass = outputSpec.getInputRowDataClass(inputIdOfOutput)
          val rowVar = prepareRowVar(row, inputVars, inputRowDataClass)

          // reuse input expr for output node
          val indices = fieldIndices(outputType)
          indices.foreach(
            index =>
              outputSpec.getOperatorCtx
                .addReusableInputUnboxingExprs(rowVar.resultTerm, index, inputVars(index)))
          // bind downstream operator input type and input row before call its doProcessConsume
          if (inputIdOfOutput == 1) {
            outputSpec.getExprCodeGenerator
              .bindInput(outputType, rowVar.resultTerm, inputFieldMapping = Option(indices))
          } else {
            outputSpec.getExprCodeGenerator
              .bindSecondInput(outputType, rowVar.resultTerm, inputFieldMapping = Option(indices))
          }

          // need to copy composite type such as varchar for each output if has multiple output
          val (deepCopyLocalVariable, deepCopyCode) = if (outputs.length > 1) {
            val copiedRowVarTerm = newName("copiedRowVar")
            getOperatorCtx.startNewLocalVariableStatement(copiedRowVarTerm)
            (
              getOperatorCtx.reuseLocalVariableCode(copiedRowVarTerm),
              inputVars.map(_.deepCopyMutableExpr(getOperatorCtx)).mkString("\n"))
          } else {
            ("", "")
          }
          // always pass column vars and row var to output op simultaneously, the output decide to use which one
          s"""
             |$evaluatedReqVars
             |$deepCopyLocalVariable
             |$deepCopyCode
             | // consume code
             |${outputSpec.doProcessConsume(inputIdOfOutput, toJava(inputVars), rowVar)}
             |""".stripMargin
        })
      .mkString("\n")

    s"""
       |// declare the local variable
       |$localVariables
       |$evaluatedAllVars
       |$consumeCode
     """.stripMargin
  }

  def endInputProduce(fusionCtx: CodeGeneratorContext): Unit = {
    if (!hasEndInputProduceTraversed) {
      opFusionCodegenSpec.doEndInputProduce(fusionCtx)
      hasEndInputProduceTraversed = true
    }
  }

  def endInputConsume(): String = {
    s"""
       |${outputs.map(op => op._2.getOpFusionCodegenSpec.doEndInputConsume(op._1)).mkString("\n")}
     """.stripMargin
  }

  /**
   * If current operator processConsume method doesn't pass row to downstream, we need to use the
   * outputRowTerm to prepare rowVar.
   */
  private def getOutputRowTerm(row: String): String =
    if (row != null) {
      row
    } else {
      outputRowTerm
    }

  /**
   * Prepare the rowVar of downstream operator needed, we always pass colVars and rowVars to
   * downstream simultaneously, use which one is decide by itself.
   */
  private def prepareRowVar(
      row: String,
      colVars: Seq[GeneratedExpression],
      rowTypeClazz: Class[_ <: RowData]): GeneratedExpression = {
    if (row != null) {
      new GeneratedExpression(row, NEVER_NULL, NO_CODE, outputType)
    } else {
      getExprCodeGenerator.generateResultExpression(
        // need copy the colVars first to avoid it code is used during generate row
        colVars.map(_.copyExpr),
        outputType,
        rowTypeClazz,
        getOutputRowTerm(row),
        Some(newName(variablePrefix + DEFAULT_OUT_RECORD_WRITER_TERM))
      )
    }
  }

  /**
   * Returns source code to evaluate the variables for required attributes, to prevent them to be
   * evaluated twice.
   */
  private def evaluateRequiredVariables(
      inputVars: Seq[GeneratedExpression],
      requiredVariables: java.util.Set[Integer]): String = {
    val evaluateVars = new StringBuilder
    requiredVariables.foreach(
      index => {
        val expr = inputVars(index)
        if (!expr.codeUsed) {
          evaluateVars.append(expr.getCode + "\n")
        }
      })
    evaluateVars.toString()
  }

  def getOperatorCtx: CodeGeneratorContext = opFusionCodegenSpec.getOperatorCtx

  def getExprCodeGenerator: ExprCodeGenerator = opFusionCodegenSpec.getExprCodeGenerator

  def variablePrefix: String = opFusionCodegenSpec.variablePrefix
}
