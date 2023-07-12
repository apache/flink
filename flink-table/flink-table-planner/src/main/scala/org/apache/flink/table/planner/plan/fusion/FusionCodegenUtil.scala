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

import org.apache.flink.streaming.api.operators.{Input, InputSelection, StreamOperatorParameters}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenException, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, newName, newNames, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.GeneratedExpression.NO_CODE
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{OUT_ELEMENT, STREAM_RECORD}
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator.Context
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toJava
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.fusion.{FusionStreamOperatorBase, OperatorFusionCodegenFactory}
import org.apache.flink.table.runtime.operators.multipleinput.input.{InputSelectionHandler, InputSelectionSpec}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldTypes

import org.apache.calcite.rex.{RexInputRef, RexNode, RexVisitorImpl}

import java.util

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

object FusionCodegenUtil {

  def generateFusionOperator(
      outputGenerator: OpFusionCodegenSpecGenerator,
      inputSpecs: util.List[InputSelectionSpec]): (OperatorFusionCodegenFactory[RowData], Long) = {
    // Must initialize operator managedMemoryFraction before produce-consume call, codegen need it
    val (opSpecGenerators, totalManagedMemory) = setupOpSpecGenerator(outputGenerator)

    val fusionCtx = new CodeGeneratorContext(
      outputGenerator.getOpFusionCodegenSpec.getCodeGeneratorContext.tableConfig,
      outputGenerator.getOpFusionCodegenSpec.getCodeGeneratorContext.classLoader)

    // generate process code
    outputGenerator.processProduce(fusionCtx)
    // generate endInput code
    outputGenerator.endInputProduce(fusionCtx)

    var iter = opSpecGenerators.iterator
    while (iter.hasNext) {
      val op = iter.next
      // init code
      op.addReusableInitCode(fusionCtx)
      // open code from sink to leaf
      op.addReusableOpenCode(fusionCtx)
    }

    iter = opSpecGenerators.descendingIterator
    while (iter.hasNext) {
      val op = iter.next
      // close code from leaf to sink
      op.addReusableCloseCode(fusionCtx)
    }

    fusionCtx.addReusableMember(
      s"private final $STREAM_RECORD $OUT_ELEMENT = new $STREAM_RECORD(null);")
    fusionCtx.addReusableMember(
      s"private final ${className[InputSelectionHandler]} inputSelectionHandler;")

    val inputSpecRefs = fusionCtx.addReusableObject(inputSpecs, "inputSpecRefs")
    fusionCtx.addReusableInitStatement(
      s"this.inputSelectionHandler = new ${className[InputSelectionHandler]}($inputSpecRefs);")

    val operatorName = newName("FusionStreamOperator")
    val operatorCode =
      s"""
      public final class $operatorName extends ${className[FusionStreamOperatorBase]} {

        ${fusionCtx.reuseMemberCode()}

        public $operatorName(
          Object[] references,
          ${className[StreamOperatorParameters[_]]} parameters) throws Exception {
          super(parameters, ${inputSpecs.size()});
          ${fusionCtx.reuseInitCode()}
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${fusionCtx.reuseOpenCode()}
        }

        @Override
        public ${className[util.List[Input[_]]]} getInputs() {
          return ${className[util.Arrays]}.asList(${fusionCtx.reuseFusionProcessCode()});
        }

        @Override
        public void endInput(int inputId) throws Exception {
          inputSelectionHandler.endInput(inputId);
          ${fusionCtx.reuseFusionEndInputCode("inputId")}
        }
        
        @Override
        public ${className[InputSelection]} nextSelection() {
          return inputSelectionHandler.getInputSelection();
        }

        @Override
        public void finish() throws Exception {
            ${fusionCtx.reuseFinishCode()}
            super.finish();
        }

        @Override
        public void close() throws Exception {
           super.close();
           ${fusionCtx.reuseCloseCode()}
        }

        ${fusionCtx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    (
      new OperatorFusionCodegenFactory(
        new GeneratedOperator(
          operatorName,
          operatorCode,
          fusionCtx.references.toArray,
          fusionCtx.tableConfig)),
      totalManagedMemory)
  }

  private def setupOpSpecGenerator(outputGenerator: OpFusionCodegenSpecGenerator)
      : (util.ArrayDeque[OpFusionCodegenSpecGenerator], Long) = {
    val opSpecGenerators = new util.ArrayDeque[OpFusionCodegenSpecGenerator]
    getAllOpSpecGenerator(outputGenerator, opSpecGenerators)

    var totalManagedMemory: Long = 0
    opSpecGenerators.forEach(op => totalManagedMemory += op.getManagedMemory)

    // setup the operator generator needed context before call produce-consume
    opSpecGenerators.forEach(
      op => {
        val context = new Context {
          override def getManagedMemoryFraction: Double = if (totalManagedMemory != 0) {
            op.getManagedMemory * 1.0 / totalManagedMemory
          } else {
            0
          }
        }

        op.setup(context)
      })

    (opSpecGenerators, totalManagedMemory)
  }

  private def getAllOpSpecGenerator(
      outputOp: OpFusionCodegenSpecGenerator,
      operators: util.ArrayDeque[OpFusionCodegenSpecGenerator]): Unit = {
    operators.add(outputOp)
    // visit all input operator from output to leaf recursively
    outputOp.getInputs.foreach(op => getAllOpSpecGenerator(op, operators))
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent them
   * to be evaluated twice.
   */
  def evaluateVariables(varExprs: Seq[GeneratedExpression]): String = {
    val evaluate = varExprs.filter(_.code.nonEmpty).map(_.code).mkString("\n")
    varExprs.foreach(_.code = NO_CODE)
    evaluate
  }

  /**
   * Returns source code to evaluate the variables for required attributes, to prevent them to be
   * evaluated twice.
   */
  def evaluateRequiredVariables(
      varExprs: Seq[GeneratedExpression],
      requiredVarIndices: Set[Int]): String = {
    val evaluateVars = new StringBuilder
    requiredVarIndices.foreach(
      index => {
        val expr = varExprs(index)
        if (expr.code.nonEmpty) {
          evaluateVars.append(expr.code + "\n")
          expr.code = NO_CODE
        }
      })
    evaluateVars.toString()
  }

  def extractRefInputFields(
      exprs: Seq[RexNode],
      input1Type: LogicalType,
      input2Type: LogicalType): (Set[Int], Set[Int]) = {
    val visitor = new InputRefVisitor(input1Type, Option(input2Type))
    // extract referenced input fields from expressions
    exprs.foreach(_.accept(visitor))
    (visitor.input1Fields.toSet, visitor.input2Fields.toSet)
  }

  def extractRefInputFields(exprs: Seq[RexNode], input1Type: LogicalType): Set[Int] = {
    val visitor = new InputRefVisitor(input1Type)
    // extract referenced input fields from expressions
    exprs.foreach(_.accept(visitor))
    visitor.input1Fields.toSet
  }

  /** Constructing the consumeFunction according to resultType. */
  def constructDoConsumeFunction(
      prefix: String,
      opCodegenCtx: CodeGeneratorContext,
      fusionContext: OpFusionContext,
      resultType: RowType): String = {
    val parameters = mutable.ArrayBuffer[String]()
    val paramVars = mutable.ArrayBuffer[GeneratedExpression]()
    val consumeFunctionName = newName(prefix + "DoConsume")
    for (i <- 0 until resultType.getFieldCount) {
      val paramType = getFieldTypes(resultType).get(i)
      val paramTypeTerm = primitiveTypeTermForType(paramType)
      val Seq(paramTerm, paramNullTerm) = newNames("field", "isNull")

      parameters += s"$paramTypeTerm $paramTerm"
      parameters += s"boolean $paramNullTerm"

      paramVars += GeneratedExpression(paramTerm, paramNullTerm, NO_CODE, paramType)
    }

    opCodegenCtx.addReusableMember(
      s"""
         | private void $consumeFunctionName(${parameters.mkString(", ")}) throws Exception {
         |   ${fusionContext.processConsume(toJava(paramVars))}
         | }
       """.stripMargin
    )

    consumeFunctionName
  }

  /** Constructing the consume code according to consumeFunctionName and resultVars. */
  def constructDoConsumeCode(
      consumeFunctionName: String,
      resultVars: Seq[GeneratedExpression]): String = {
    val arguments = mutable.ArrayBuffer[String]()
    resultVars.foreach {
      case expr =>
        arguments += expr.resultTerm
        arguments += expr.nullTerm
    }
    s"""
       | ${evaluateVariables(resultVars)}
       | $consumeFunctionName(${arguments.mkString(", ")});
     """.stripMargin
  }
}

/** An RexVisitor to extract all referenced input fields */
class InputRefVisitor(input1Type: LogicalType, input2Type: Option[LogicalType] = None)
  extends RexVisitorImpl[Unit](true) {

  val input1Fields = mutable.ListBuffer[Int]()
  val input2Fields = mutable.ListBuffer[Int]()

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    val input1Arity = input1Type match {
      case r: RowType => r.getFieldCount
      case _ => 1
    }
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    if (inputRef.getIndex < input1Arity) {
      input1Fields += inputRef.getIndex
    } else {
      input2Type.getOrElse(throw new CodeGenException("Invalid input access."))
      input2Fields += inputRef.getIndex - input1Arity
    }
  }
}
