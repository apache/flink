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

package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.runtime.util.SingleElementIterator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.expressions.ApiExpressionUtils.localRef
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.STREAM_RECORD
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.context.ExecutionContextImpl
import org.apache.flink.table.runtime.generated.{GeneratedAggsHandleFunction, GeneratedOperator}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter._
import org.apache.flink.table.runtime.typeutils.InternalSerializers
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{DistinctType, LogicalType, RowType}

import org.apache.calcite.tools.RelBuilder

import scala.annotation.tailrec

/**
  * Batch aggregate code generate helper.
  */
object AggCodeGenHelper {

  def getAggBufferNames(
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo])
    : Array[Array[String]] = {
    val auxGroupingNames = auxGrouping.indices
      .map(index => Array(s"aux_group$index"))

    val aggNames = aggInfos.map { aggInfo =>

      val aggBufferIdx = auxGrouping.length + aggInfo.aggIndex

      aggInfo.function match {

        // create one buffer for each attribute in declarative functions
        case function: DeclarativeAggregateFunction =>
          function.aggBufferAttributes.map(attr => s"agg${aggBufferIdx}_${attr.getName}")

        // create one buffer for imperative functions
        case _: AggregateFunction[_, _] =>
          Array(s"agg$aggBufferIdx")
      }
    }

    (auxGroupingNames ++ aggNames).toArray
  }

  def getAggBufferTypes(
      inputType: RowType,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo])
    : Array[Array[LogicalType]] = {
    val auxGroupingTypes = auxGrouping
      .map { index =>
        Array(inputType.getTypeAt(index))
      }

    val aggTypes = aggInfos
      .map(_.externalAccTypes.map(fromDataTypeToLogicalType))

    auxGroupingTypes ++ aggTypes
  }

  def getFunctionIdentifiers(aggInfos: Seq[AggregateInfo]): Map[AggregateFunction[_, _], String] = {
    aggInfos
        .map(_.function)
        .filter(a => a.isInstanceOf[AggregateFunction[_, _]])
        .map(a => a -> CodeGenUtils.udfFieldName(a)).toMap
        .asInstanceOf[Map[AggregateFunction[_, _], String]]
  }

  def projectRowType(rowType: RowType, mapping: Array[Int]): RowType = {
    RowType.of(mapping.map(rowType.getTypeAt), mapping.map(rowType.getFieldNames.get(_)))
  }

  /**
    * Add agg handler to class member and open it.
    */
  private[flink] def addAggsHandler(
      aggsHandler: GeneratedAggsHandleFunction,
      ctx: CodeGeneratorContext,
      aggsHandlerCtx: CodeGeneratorContext)
    : String = {
    ctx.addReusableInnerClass(aggsHandler.getClassName, aggsHandler.getCode)
    val handler = CodeGenUtils.newName("handler")
    ctx.addReusableMember(s"${aggsHandler.getClassName} $handler = null;")
    val aggRefers = ctx.addReusableObject(aggsHandlerCtx.references.toArray, "Object[]")
    ctx.addReusableOpenStatement(
      s"""
         |$handler = new ${aggsHandler.getClassName}($aggRefers);
         |$handler.open(new ${classOf[ExecutionContextImpl].getCanonicalName}(
         |  this, getRuntimeContext()));
       """.stripMargin)
    ctx.addReusableCloseStatement(s"$handler.close();")
    handler
  }

  /**
    * The generated codes only supports the comparison of the key terms
    * in the form of binary row with only one memory segment.
    */
  private[flink] def genGroupKeyChangedCheckCode(
      currentKeyTerm: String,
      lastKeyTerm: String)
    : String = {
    s"""
       |$currentKeyTerm.getSizeInBytes() != $lastKeyTerm.getSizeInBytes() ||
       |  !(org.apache.flink.table.data.binary.BinaryRowDataUtil.byteArrayEquals(
       |     $currentKeyTerm.getSegments()[0].getHeapMemory(),
       |     $lastKeyTerm.getSegments()[0].getHeapMemory(),
       |     $currentKeyTerm.getSizeInBytes()))
       """.stripMargin.trim
  }

  def genSortAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      inputTerm: String,
      inputType: RowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      outputType: RowType,
      forHashAgg: Boolean = false)
    : (String, String, GeneratedExpression) = {
    // gen code to apply aggregate functions to grouping elements
    val argsMapping = buildAggregateArgsMapping(
      isMerge, grouping.length, inputType, auxGrouping, aggInfos, aggBufferTypes)

    val aggBufferExprs = genFlatAggBufferExprs(
      isMerge,
      ctx,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBufferNames,
      aggBufferTypes)

    val initAggBufferCode = genInitFlatAggregateBuffer(
      ctx,
      builder,
      inputType,
      inputTerm,
      grouping,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      aggBufferExprs,
      forHashAgg)

    val doAggregateCode = genAggregateByFlatAggregateBuffer(
      isMerge,
      ctx,
      builder,
      inputType,
      inputTerm,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      argsMapping,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs)

    val aggOutputExpr = genSortAggOutputExpr(
      isMerge,
      isFinal,
      ctx,
      builder,
      grouping,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      argsMapping,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs,
      outputType)

    (initAggBufferCode, doAggregateCode, aggOutputExpr)
  }

  /**
    * Build an arg mapping for reference binding. The mapping will be a 2-dimension array.
    * The first dimension represents the aggregate index, the order is same with agg calls in plan.
    * The second dimension information represents input count of the aggregate. The meaning will
    * be different depends on whether we should do merge.
    *
    * In non-merge case, aggregate functions will treat inputs as operands. In merge case, the
    * input is local aggregation's buffer, we need to merge with our local aggregate buffers.
    */
  private[flink] def buildAggregateArgsMapping(
      isMerge: Boolean,
      aggBufferOffset: Int,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      aggBufferTypes: Array[Array[LogicalType]])
    : Array[Array[(Int, LogicalType)]] = {
    val aggArgs = aggInfos.map(_.argIndexes).toArray
    val auxGroupingMapping = auxGrouping.indices.map {
      i => Array[(Int, LogicalType)]((i, aggBufferTypes(i)(0)))
    }.toArray

    val aggCallMapping = if (isMerge) {
      var offset = aggBufferOffset + auxGrouping.length
      aggBufferTypes.slice(auxGrouping.length, aggBufferTypes.length).map { types =>
        val baseOffset = offset
        offset = offset + types.length
        types.indices.map(index => (baseOffset + index, types(index))).toArray
      }
    } else {
      aggArgs.map(args => args.map(i => (i, inputType.getTypeAt(i))))
    }

    auxGroupingMapping ++ aggCallMapping
  }

  def newLocalReference(resultTerm: String, resultType: LogicalType): LocalReferenceExpression = {
    localRef(resultTerm, fromLogicalTypeToDataType(resultType))
  }

  /**
    * Resolves the given expression to a resolved Expression.
    *
    * @param isMerge this is called from merge() method
    */
  private case class ResolveReference(
      ctx: CodeGeneratorContext,
      relBuilder: RelBuilder,
      isMerge: Boolean,
      agg: DeclarativeAggregateFunction,
      aggIndex: Int,
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferTypes: Array[Array[LogicalType]])
    extends DeclarativeExpressionResolver(relBuilder, agg, isMerge) {

    override def toMergeInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      toRexInputRef(relBuilder, inputIndex, inputType)
    }

    override def toAccInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      toRexInputRef(relBuilder, inputIndex, inputType)
    }

    override def toAggBufferExpr(name: String, localIndex: Int): ResolvedExpression = {
      val variableName = s"agg${aggIndex}_$name"
      newLocalReference(variableName, aggBufferTypes(aggIndex)(localIndex))
    }
  }

  /**
    * Declare all aggregate buffer variables, store these variables in class members
    */
  private[flink] def genFlatAggBufferExprs(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]]): Seq[GeneratedExpression] = {
    val exprCodeGen = new ExprCodeGenerator(ctx, false)
    val converter = new ExpressionConverter(builder)

    val accessAuxGroupingExprs = auxGrouping.indices
      .map(idx => newLocalReference(aggBufferNames(idx).head, aggBufferTypes(idx).head))

    val aggCallExprs = aggInfos.flatMap { aggInfo =>

      val aggBufferIdx = auxGrouping.length + aggInfo.aggIndex

      aggInfo.function match {

        // create a buffer expression for each attribute in declarative functions
        case function: DeclarativeAggregateFunction =>
          val ref = ResolveReference(
            ctx,
            builder,
            isMerge,
            function,
            aggBufferIdx,
            argsMapping,
            aggBufferTypes)
          function.aggBufferAttributes().map(_.accept(ref))

        // create one buffer for imperative functions
        case _: AggregateFunction[_, _] =>
          val aggBufferName = aggBufferNames(aggBufferIdx).head
          val aggBufferType = aggBufferTypes(aggBufferIdx).head
          Some(newLocalReference(aggBufferName, aggBufferType))
      }
    }

    val aggBufferExprs = accessAuxGroupingExprs ++ aggCallExprs

    aggBufferExprs
      .map(_.accept(converter))
      .map(exprCodeGen.generateExpression)
  }

  /**
    * Generate codes which will init the aggregate buffer.
    */
  private[flink] def genInitFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputType: RowType,
      inputTerm: String,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      aggBufferExprs: Seq[GeneratedExpression],
      forHashAgg: Boolean = false)
    : String = {
    val exprCodeGen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm, inputFieldMapping = Some(auxGrouping))
    val converter = new ExpressionConverter(builder)

    val initAuxGroupingExprs = {
      if (forHashAgg) {
        // access fallbackInput
        auxGrouping.indices.map(idx => idx + grouping.length).toArray
      } else {
        // access input
        auxGrouping
      }
    }.map { idx =>
      GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm, idx)
    }

    val initAggCallBufferExprs = aggInfos.flatMap { aggInfo =>
      aggInfo.function match {

        // generate code for each agg buffer in declarative functions
        case function: DeclarativeAggregateFunction =>
          val expressions = function.initialValuesExpressions
          val rexNodes = expressions.map(_.accept(converter))
          rexNodes.map(exprCodeGen.generateExpression)

        // call createAccumulator() in imperative functions
        case function: AggregateFunction[_, _] =>
          val accTerm = s"${functionIdentifiers(function)}.createAccumulator()"
          val externalAccType = aggInfo.externalAccTypes.head
          val internalAccType = externalAccType.getLogicalType
          val genExpr = GeneratedExpression(
            genToInternalConverter(ctx, externalAccType)(accTerm),
            NEVER_NULL,
            NO_CODE,
            internalAccType)
          Seq(genExpr)
        }
    }

    val initAggBufferExprs = initAuxGroupingExprs ++ initAggCallBufferExprs
    require(aggBufferExprs.length == initAggBufferExprs.length)

    aggBufferExprs.zip(initAggBufferExprs).map {
      case (aggBufVar, initExpr) =>
        val resultCode = genElementCopyTerm(ctx, aggBufVar.resultType, initExpr.resultTerm)
        s"""
           |${initExpr.code}
           |${aggBufVar.nullTerm} = ${initExpr.nullTerm};
           |${aggBufVar.resultTerm} = $resultCode;
         """.stripMargin.trim
    } mkString "\n"
  }

  @tailrec
  private def genElementCopyTerm(
      ctx: CodeGeneratorContext,
      t: LogicalType,
      inputTerm: String)
  : String = t.getTypeRoot match {
    case CHAR | VARCHAR | ARRAY | MULTISET | MAP | ROW | STRUCTURED_TYPE =>
      val serializer = InternalSerializers.create(t)
      val term = ctx.addReusableObject(
        serializer, "serializer", serializer.getClass.getCanonicalName)
      val typeTerm = boxedTypeTermForType(t)
      s"($typeTerm) $term.copy($inputTerm)"
    case DISTINCT_TYPE =>
      genElementCopyTerm(ctx, t.asInstanceOf[DistinctType].getSourceType, inputTerm)
    case _ => inputTerm
  }

  private[flink] def genAggregateByFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputType: RowType,
      inputTerm: String,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression])
    : String = {
    if (isMerge) {
      genMergeFlatAggregateBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        auxGrouping,
        aggInfos,
        functionIdentifiers,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        aggBufferExprs)
    } else {
      genAccumulateFlatAggregateBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        auxGrouping,
        aggInfos,
        functionIdentifiers,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        aggBufferExprs)
    }
  }

  def genSortAggOutputExpr(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression],
      outputType: RowType)
    : GeneratedExpression = {
    val valueRow = CodeGenUtils.newName("valueRow")
    val resultCodeGen = new ExprCodeGenerator(ctx, false)
    if (isFinal) {
      val getValueExprs = genGetValueFromFlatAggregateBuffer(
        isMerge,
        ctx,
        builder,
        auxGrouping,
        aggInfos,
        functionIdentifiers,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        outputType)
      val valueRowType = RowType.of(getValueExprs.map(_.resultType): _*)
      resultCodeGen.generateResultExpression(
        getValueExprs, valueRowType, classOf[GenericRowData], valueRow)
    } else {
      val valueRowType = RowType.of(aggBufferExprs.map(_.resultType): _*)
      resultCodeGen.generateResultExpression(
        aggBufferExprs, valueRowType, classOf[GenericRowData], valueRow)
    }
  }

  /**
    * Generate expressions which will get final aggregate value from aggregate buffers.
    */
  private[flink] def genGetValueFromFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      outputType: RowType): Seq[GeneratedExpression] = {
    val exprCodeGen = new ExprCodeGenerator(ctx, false)
    val converter = new ExpressionConverter(builder)

    val auxGroupingExprs = auxGrouping.indices.map { idx =>
      val aggBufferName = aggBufferNames(idx).head
      val aggBufferType = aggBufferTypes(idx).head
      val nullTerm = s"${aggBufferName}IsNull"
      GeneratedExpression(aggBufferName, nullTerm, NO_CODE, aggBufferType)
    }

    val getValueExprs = aggInfos.map { aggInfo =>

      val aggBufferIdx = auxGrouping.length + aggInfo.aggIndex

      aggInfo.function match {

        // evaluate the value expression in declarative functions
        case function: DeclarativeAggregateFunction =>
          val ref = ResolveReference(
            ctx,
            builder,
            isMerge,
            function,
            aggBufferIdx,
            argsMapping,
            aggBufferTypes)
          val getValueRexNode = function.getValueExpression
            .accept(ref)
            .accept(converter)
          exprCodeGen.generateExpression(getValueRexNode)

        // call getValue() for imperative functions
        case function: AggregateFunction[_, _] =>
          val aggBufferName = aggBufferNames(aggBufferIdx).head
          val externalAccType = aggInfo.externalAccTypes.head
          val externalResultType = aggInfo.externalResultType
          val resultType = externalResultType.getLogicalType
          val getValueCode = s"${functionIdentifiers(function)}.getValue(" +
            s"${genToExternalConverter(ctx, externalAccType, aggBufferName)})"
          val resultTerm = genToInternalConverter(ctx, externalResultType)(getValueCode)
          val nullTerm = s"${aggBufferName}IsNull"
          GeneratedExpression(resultTerm, nullTerm, NO_CODE, resultType)
      }
    }

    auxGroupingExprs ++ getValueExprs
  }

  /**
    * Generate codes which will read input and merge the aggregate buffers.
    */
  private[flink] def genMergeFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression])
    : String = {
    val exprCodeGen = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = inputTerm)
    val converter = new ExpressionConverter(builder)

    var currentAggBufferExprIdx = auxGrouping.length

    val mergeCode = aggInfos.map { aggInfo =>

      val aggBufferIdx = auxGrouping.length + aggInfo.aggIndex

      aggInfo.function match {

        // merge each agg buffer for declarative functions
        case function: DeclarativeAggregateFunction =>
          val ref = ResolveReference(
            ctx,
            builder,
            isMerge = true,
            function,
            aggBufferIdx,
            argsMapping,
            aggBufferTypes)
          val mergeExprs = function.mergeExpressions
            .map(_.accept(ref))
            .map(_.accept(converter))
            .map(exprCodeGen.generateExpression)
          mergeExprs
            .map { mergeExpr =>
              val aggBufferExpr = aggBufferExprs(currentAggBufferExprIdx)
              currentAggBufferExprIdx += 1
              s"""
                 |${mergeExpr.code}
                 |${aggBufferExpr.nullTerm} = ${mergeExpr.nullTerm};
                 |if (!${mergeExpr.nullTerm}) {
                 |  ${mergeExpr.copyResultTermToTargetIfChanged(ctx, aggBufferExpr.resultTerm)}
                 |}
              """.stripMargin
            }
            .mkString("\n")

        // call merge() for imperative functions
        case function: AggregateFunction[_, _] =>
          val (inputIndex, inputType) = argsMapping(aggBufferIdx).head
          val inputRef = toRexInputRef(builder, inputIndex, inputType)
          val inputExpr = exprCodeGen.generateExpression(
            inputRef.accept(converter))
          val aggBufferName = aggBufferNames(aggBufferIdx).head
          val aggBufferExpr = aggBufferExprs(currentAggBufferExprIdx)
          currentAggBufferExprIdx += 1
          val iterableTypeTerm = className[SingleElementIterator[_]]
          val externalAccType = aggInfo.externalAccTypes.head
          val externalAccTypeTerm = typeTerm(externalAccType.getConversionClass)
          val externalAccTerm = newName("acc")
          val aggIndex = aggInfo.aggIndex
          s"""
            |$iterableTypeTerm accIt$aggIndex = new $iterableTypeTerm();
            |accIt$aggIndex.set(${
              genToExternalConverter(ctx, externalAccType, inputExpr.resultTerm)});
            |$externalAccTypeTerm $externalAccTerm = ${
              genToExternalConverter(ctx, externalAccType, aggBufferName)};
            |${functionIdentifiers(function)}.merge($externalAccTerm, accIt$aggIndex);
            |$aggBufferName = ${genToInternalConverter(ctx, externalAccType)(externalAccTerm)};
            |${aggBufferExpr.nullTerm} = ${aggBufferName}IsNull || ${inputExpr.nullTerm};
          """.stripMargin
      }
    }

    mergeCode.mkString("\n")
  }

  /**
    * Generate codes which will read input and accumulating aggregate buffers.
    */
  private[flink] def genAccumulateFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggInfos: Seq[AggregateInfo],
      functionIdentifiers: Map[AggregateFunction[_, _], String],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      aggBufferExprs: Seq[GeneratedExpression])
    : String = {
    val exprCodeGen = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = inputTerm)
    val converter = new ExpressionConverter(builder)

    var currentAggBufferExprIdx = auxGrouping.length

    val filteredAccCode = aggInfos.map { aggInfo =>

      val aggCall = aggInfo.agg

      val aggBufferIdx = auxGrouping.length + aggInfo.aggIndex

      val accCode = aggInfo.function match {

        // update each agg buffer for declarative functions
        case function: DeclarativeAggregateFunction =>
          val ref = ResolveReference(
            ctx,
            builder,
            isMerge = false,
            function,
            aggBufferIdx,
            argsMapping,
            aggBufferTypes)
          val accExprs = function.accumulateExpressions
            .map(_.accept(ref))
            .map(_.accept(new ExpressionConverter(builder)))
            .map(exprCodeGen.generateExpression)
          accExprs
            .map { accExpr =>
              val aggBufferExpr = aggBufferExprs(currentAggBufferExprIdx)
              currentAggBufferExprIdx += 1
              s"""
                |${accExpr.code}
                |${aggBufferExpr.nullTerm} = ${accExpr.nullTerm};
                |if (!${accExpr.nullTerm}) {
                |  // copy result term
                |  ${accExpr.copyResultTermToTargetIfChanged(ctx, aggBufferExpr.resultTerm)}
                |}
              """.stripMargin
            }
            .mkString("\n")

        // call accumulate() for imperative functions
        case function: AggregateFunction[_, _] =>
          val args = argsMapping(aggBufferIdx)
          val inputExprs = args.map { case (argIndex, argType) =>
              val inputRef = toRexInputRef(builder, argIndex, argType)
              exprCodeGen.generateExpression(inputRef.accept(converter))
          }
          val operandTerms = inputExprs.zipWithIndex.map { case (expr, i) =>
              genToExternalConverterAll(ctx, aggInfo.externalArgTypes(i), expr)
          }
          val aggBufferName = aggBufferNames(aggBufferIdx).head
          val aggBufferExpr = aggBufferExprs(currentAggBufferExprIdx)
          currentAggBufferExprIdx += 1
          val externalAccType = aggInfo.externalAccTypes.head
          val externalAccTypeTerm = typeTerm(externalAccType.getConversionClass)
          val externalAccTerm = newName("acc")
          val externalAccCode = genToExternalConverter(ctx, externalAccType, aggBufferName)
          s"""
            |$externalAccTypeTerm $externalAccTerm = $externalAccCode;
            |${functionIdentifiers(function)}.accumulate(
            |  $externalAccTerm,
            |  ${operandTerms.mkString(", ")});
            |$aggBufferName = ${genToInternalConverter(ctx, externalAccType)(externalAccTerm)};
            |${aggBufferExpr.nullTerm} = false;
          """.stripMargin
      }

      // apply filter if present
      if (aggInfo.agg.filterArg >= 0) {
        s"""
          |if ($inputTerm.getBoolean(${aggCall.filterArg})) {
          |  $accCode
          |}
        """.stripMargin
      } else {
        accCode
      }
    }

    filteredAccCode.mkString("\n")
  }

  /**
    * Generate a operator with adding a hasInput field to agg operator.
    */
  private[flink] def generateOperator(
      ctx: CodeGeneratorContext,
      name: String,
      operatorBaseClass: String,
      processCode: String,
      endInputCode: String,
      inputType: RowType): GeneratedOperator[OneInputStreamOperator[RowData, RowData]] = {
    ctx.addReusableMember("private boolean hasInput = false;")
    ctx.addReusableMember(s"$STREAM_RECORD element = new $STREAM_RECORD((Object)null);")
    OperatorCodeGenerator.generateOneInputStreamOperator(
      ctx,
      name,
      processCode,
      inputType,
      endInputCode = Some(endInputCode),
      lazyInputUnboxingCode = true)
  }
}
