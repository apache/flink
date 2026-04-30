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
import org.apache.flink.table.planner.calcite.{FlinkRexBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex._

import scala.collection.JavaConverters._

object CalcCodeGenerator {

  def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      inputType: RowType,
      outputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {
    // filter out time attributes
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      projection,
      condition,
      inputTerm,
      CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode = true,
      retainHeader = retainHeader,
      outputDirectly = false
    )

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
      calcProjection: Seq[RexNode],
      calcCondition: Option[RexNode],
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
      calcProjection,
      calcCondition,
      inputTerm,
      collectorTerm = collectorTerm,
      eagerInputUnboxingCode = false,
      retainHeader = false,
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
      projection: Seq[RexNode],
      condition: Option[RexNode],
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode: Boolean,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false): String = {

    // according to the SQL standard, every table function should also be a scalar function
    // but we don't allow that for now
    projection.foreach(_.accept(ScalarFunctionsValidator))
    condition.foreach(_.accept(ScalarFunctionsValidator))

    // Build a single RexProgram that contains both projections and the condition (when
    // present). RexProgramBuilder's structural CSE collapses shared sub-expressions across
    // them, and the visitor's RexLocalRef cache then deduplicates evaluation across all
    // call sites.
    val rexProgram = buildRexProgram(ctx.classLoader, inputType, projection, condition)

    val exprGenerator = new ExprCodeGenerator(ctx, false, rexProgram)
      .bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.lengthCompare(inputType.getFieldCount) == 0 &&
      projection.zipWithIndex.forall {
        case (rexNode, index) =>
          rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }

    def produceOutputCode(resultTerm: String): String = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generateCollect(resultTerm)}"
    }

    def produceProjectionCode: String = {
      val projection = rexProgram.getProjectList.asScala

      // JSON expressions need their full RexNode tree expanded before codegen because
      // JSON_OBJECT/JSON_ARRAY/JSON compose through nested calls and aren't supported by the
      // RexLocalRef path; for everything else, hand the RexLocalRef itself to generateExpression
      // so visitLocalRef both dereferences via the program and memoizes the result through
      // ctx.reusableLocalRefExprs. Without this, identical sub-expressions across projections
      // (e.g. SELECT UPPER(a), UPPER(a) FROM t) would be regenerated per projection.
      val projectionExprs = projection.map {
        case localRef: RexLocalRef if containsJson(rexProgram, localRef) =>
          exprGenerator.generateExpression(rexProgram.expandLocalRef(localRef))
        case other =>
          exprGenerator.generateExpression(other)
      }

      val projectionExpression =
        exprGenerator.generateResultExpression(projectionExprs, outRowType, outRowClass)

      val projectionExpressionCode = projectionExpression.code

      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }

      // The cached RexLocalRef bodies (ctx.reuseLocalRefCode()) are emitted once in
      // generateProcessCode, BEFORE the filter check, so that any filter referencing a shared
      // sub-expression also sees the assigned result terms.
      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }

    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionCode = produceProjectionCode
      // Cached RexLocalRef bodies are populated lazily by visitLocalRef; emit them once
      // here, after every expression has been generated, so each cached result term is
      // assigned exactly once before any consumer reads it.
      val localRefCode = ctx.reuseLocalRefCode()
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |$localRefCode
         |$projectionCode
         |""".stripMargin
    } else {
      // Routing the filter through rexProgram.getCondition() (a RexLocalRef) makes the
      // visitor hit the same reusableLocalRefExprs cache that the projections read.
      val filterCondition = exprGenerator.generateExpression(rexProgram.getCondition)
      // only filter
      if (onlyFilter) {
        val localRefCode = ctx.reuseLocalRefCode()
        s"""
           |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
           |$localRefCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        val filterInputCode = ctx.reuseInputUnboxingCode()
        val filterInputSet: Set[(String, Int)] = ctx.reusableInputUnboxingExprs.keySet.toSet

        // Snapshot of cached RexLocalRef indices populated while generating the filter.
        // Entries added later (during projection generation) stay inside the if-block,
        // so we don't compute them for rows the filter rejects.
        val filterLocalRefSet: Set[Int] = ctx.reusableLocalRefExprs.keySet.toSet

        // if any filter conditions, projection code will enter an new scope
        val projectionCode = produceProjectionCode

        val projectionInputCode = ctx.reusableInputUnboxingExprs
          .filter { case (k, _) => !filterInputSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")

        // Partition cached RexLocalRef bodies: entries used by the filter (added before the
        // snapshot above) are emitted unconditionally before the filter check so the filter can
        // read their result terms; entries added only by the projection are emitted inside the
        // if-block.
        val filterLocalRefCode = ctx.reusableLocalRefExprs
          .filter { case (k, _) => filterLocalRefSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")
        val projectionLocalRefCode = ctx.reusableLocalRefExprs
          .filter { case (k, _) => !filterLocalRefSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")

        val filterInput = if (eagerInputUnboxingCode) filterInputCode else ""
        val projectionInput = if (eagerInputUnboxingCode) projectionInputCode else ""

        s"""
           |$filterInput
           |$filterLocalRefCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  $projectionInput
           |  $projectionLocalRefCode
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
  }

  /**
   * Builds a [[RexProgram]] containing `projection` and `condition` (if non-null). The visitor's
   * [[RexLocalRef]] cache uses the program to dereference and memoize shared sub-expressions across
   * all call sites.
   */
  private def buildRexProgram(
      classLoader: ClassLoader,
      inputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode]): RexProgram = {
    val typeFactory = new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE)
    val rexBuilder = new FlinkRexBuilder(typeFactory)
    val relInputType = typeFactory.createFieldTypeFromLogicalType(inputType)
    val builder = new RexProgramBuilder(relInputType, rexBuilder)
    projection.foreach(p => builder.addProject(p, null))
    if (condition.isDefined) {
      builder.addCondition(condition.get)
    }
    builder.getProgram
  }

  private def containsJson(rexProgram: RexProgram, rexNode: RexNode): Boolean = rexNode match {
    case localRef: RexLocalRef =>
      containsJson(rexProgram, rexProgram.getExprList.get(localRef.getIndex))
    case call: RexCall =>
      val name = call.getOperator.getName
      name == "JSON" || name == "JSON_OBJECT" || name == "JSON_ARRAY" ||
      call.getOperands.asScala.exists(containsJson(rexProgram, _))
    case _ => false
  }

  private def checkProjectionIsIdentity(projection: java.util.List[RexNode]): Boolean = {
    var i = 0
    val it = projection.iterator()
    while (it.hasNext) {
      it.next() match {
        case ref: RexInputRef if ref.getIndex == i => // ok
        case _ => return false
      }
      i += 1
    }
    true
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
