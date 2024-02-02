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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.{FlatMapFunction, Function}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.codegen.GeneratedExpression.NO_CODE
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil.AccessedLocalRefsFinder
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CalcCodeGenerator {

  def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      expr: Seq[RexNode],
      projection: Seq[RexLocalRef],
      condition: Option[RexLocalRef],
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {

    val inputType = inputTransform.getOutputType
      .asInstanceOf[InternalTypeInfo[RowData]]
      .toRowType
    // filter out time attributes
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val processCode = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      expr,
      projection,
      condition,
      eagerInputUnboxingCode = true,
      retainHeader = retainHeader)

    val genOperator =
      OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
        ctx,
        opName,
        processCode,
        inputType,
        inputTerm = inputTerm,
        lazyInputUnboxingCode = true)

    new CodeGenOperatorFactory(genOperator)
  }

  private[flink] def generateFunction[T <: Function](
      inputType: RowType,
      name: String,
      returnType: RowType,
      outRowClass: Class[_ <: RowData],
      calcExpression: Seq[RexNode],
      calcProjection: Seq[RexLocalRef],
      calcCondition: Option[RexLocalRef],
      tableConfig: ReadableConfig,
      classLoader: ClassLoader): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val collectorTerm = CodeGenUtils.DEFAULT_COLLECTOR_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      returnType,
      outRowClass,
      calcExpression,
      calcProjection,
      calcCondition,
      collectorTerm = collectorTerm,
      eagerInputUnboxingCode = false,
      outputDirectly = true
    )

    FunctionCodeGenerator.generateFunction(
      ctx,
      name,
      classOf[FlatMapFunction[RowData, RowData]],
      processCode,
      returnType,
      inputType,
      input1Term = inputTerm,
      collectorTerm = collectorTerm)
  }
  private[flink] def generateProcessCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outRowType: RowType,
      outRowClass: Class[_ <: RowData],
      expr: Seq[RexNode],
      projection: Seq[RexLocalRef],
      condition: Option[RexLocalRef],
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode: Boolean,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false): String = {

    // according to the SQL standard, every table function should also be a scalar function
    // but we don't allow that for now
    expr.foreach(_.accept(ScalarFunctionsValidator))
    val conditionExpr = condition match {
      case Some(c) => Some(expr(c.getIndex))
      case _ => None
    }
    conditionExpr.foreach(_.accept(ScalarFunctionsValidator))

    ctx.initExpressions(expr)

    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.isEmpty && condition.nonEmpty

    val onlyFilterExprs =
      if (condition.isEmpty) Seq.empty
      else {
        val accessedRefsFinder = new AccessedLocalRefsFinder(JavaScalaConversionUtil.toJava(expr))
        condition.get.accept(accessedRefsFinder)
        accessedRefsFinder.getAccessedLocalRefs
      }

    val onlyProjectionExprs =
      expr.zipWithIndex.filter(e => !onlyFilterExprs.contains(e._2)).map(e => e._2)

    val generatedExpressions = expr.map(
      p => {
        val r = exprGenerator.generateExpression(p)
        //        println(ctx.orderedExpressions.size)
        r
      })

    def produceOutputCode(resultTerm: String): String = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generateCollect(resultTerm)}"
    }

    def generateCode(exprIdx: Seq[Int]): String = {
      val generatedExpressionCodes = exprIdx
        .filter(p => generatedExpressions(p) != null)
        .map(p => (generatedExpressions(p), p))
        .map(
          e => {
            "// EXPR %d Starts \n%s\n// EXPRT %d ENDS".format(e._2, e._1.getExprReuseCode, e._2)
          })
        .mkString("\n")
      generatedExpressionCodes
    }

    def produceProjectionCode: String = {
      //      val projectionExprs = expr.map(p => {
      //        println(p)
      //        exprGenerator.generateExpression(p)
      //        println("asd")
      //      })

      //      ctx.accessedLocalRefs.clear()

      val generatedExpressionCodes = generateCode(onlyProjectionExprs)
      val generatedProjectExpressions = projection
        .map(p => ctx.getReusableRexNodeExpr(p))
        .map(
          ge =>
            new GeneratedExpression(
              ge.get.resultTerm,
              ge.get.nullTerm,
              NO_CODE,
              ge.get.resultType,
              ge.get.literalValue))

      //      val projectGeneratedExpr = projection.map(p => ctx.getReusableRexNodeExpr(p).get)
      val projectionExpression =
        exprGenerator.generateResultExpression(generatedProjectExpressions, outRowType, outRowClass)

      val projectionExpressionCode = projectionExpression.getExprReuseCode

      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }

      s"""
         |$header
         |$generatedExpressionCodes
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }

    //    val ptest =
    //      exprGenerator.gen1(tt2, outRowType, outRowClass)

    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionCode = produceProjectionCode
      s"""
         |$projectionCode
         |""".stripMargin
    } else {
      // only filter
      if (onlyFilter) {
        val filterCondition = exprGenerator.generateExpression(conditionExpr.get)
        s"""
           |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
           |${filterCondition.getExprReuseCode}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection

        // if any filter conditions, projection code will enter an new scope
        val projectionCode = produceProjectionCode
        val filterCode = generateCode(onlyFilterExprs)
        val filterResultTerm = generatedExpressions(condition.get.getIndex).resultTerm
//        val filterCondition = exprGenerator.generateExpression(conditionExpr.get)
//        val filterInputCode = ctx.reuseInputUnboxingCode()
        //        val origFilterExpression = ctx.getReusableRexNodeExpr(condition.get).get
        //        val generatedFilterExpression = new GeneratedExpression(origFilterExpression.resultTerm, origFilterExpression.nullTerm, NO_CODE, origFilterExpression.resultType, origFilterExpression.literalValue)
        //        val filterInputCode = exprGenerator.generateExpression(conditionExpr.get)

//        val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)

//        val projectionInputCode = ctx.reusableInputUnboxingExprs
//          .filter(entry => !filterInputSet.contains(entry._1))
//          .values
//          .map(_.code)
//          .mkString("\n")
        s"""
           |$filterCode
           |if ($filterResultTerm) {
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
  }

  private object ScalarFunctionsValidator extends RexVisitorImpl[Unit](true) {
    override def visitCall(call: RexCall): Unit = {
      super.visitCall(call)
      call.getOperator match {
        case bsf: BridgingSqlFunction if bsf.getDefinition.getKind != FunctionKind.SCALAR =>
          throw new ValidationException(
            s"Invalid use of function '$bsf'. " +
              s"Currently, only scalar functions can be used in a projection or filter operation.")
        case _ => // ok
      }
    }
  }
}
