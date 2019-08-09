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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.metrics.Gauge
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.expressions.utils.ApiExpressionUtils
import org.apache.flink.table.expressions.{Expression, ExpressionVisitor, FieldReferenceExpression, TypeLiteralExpression, UnresolvedCallExpression, UnresolvedReferenceExpression, ValueLiteralExpression, _}
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{binaryRowFieldSetAccess, binaryRowSetNull}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.agg.batch.AggCodeGenHelper.buildAggregateArgsMapping
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator
import org.apache.flink.table.planner.expressions.{ResolvedAggInputReference, RexNodeConverter}
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.utils.SortUtil
import org.apache.flink.table.runtime.generated.{NormalizedKeyComputer, RecordComparator}
import org.apache.flink.table.runtime.operators.aggregate.{BytesHashMap, BytesHashMapSpillMemorySegmentPool}
import org.apache.flink.table.runtime.operators.sort.BufferedKVExternalSorter
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._

object HashAggCodeGenHelper {

  def prepareHashAggKVTypes(
      ctx: CodeGeneratorContext,
      aggMapKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      aggMapKeyType: RowType,
      aggBufferType: RowType): Unit = {
    ctx.addReusableObjectWithName(
      aggMapKeyType.getChildren.toArray(Array[LogicalType]()),
      aggMapKeyTypesTerm)
    ctx.addReusableObjectWithName(
      aggBufferType.getChildren.toArray(Array[LogicalType]()),
      aggBufferTypesTerm)
  }

  private[flink] def prepareHashAggMap(
      ctx: CodeGeneratorContext,
      reservedManagedMemory: Long,
      groupKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      aggregateMapTerm: String): Unit = {
    // create aggregate map
    val mapTypeTerm = classOf[BytesHashMap].getName
    ctx.addReusableMember(s"private transient $mapTypeTerm $aggregateMapTerm;")
    ctx.addReusableOpenStatement(s"$aggregateMapTerm " +
        s"= new $mapTypeTerm(" +
        s"this.getContainingTask()," +
        s"this.getContainingTask().getEnvironment().getMemoryManager()," +
        s"${reservedManagedMemory}L," +
        s" $groupKeyTypesTerm," +
        s" $aggBufferTypesTerm);")
    // close aggregate map and release memory segments
    ctx.addReusableCloseStatement(s"$aggregateMapTerm.free();")
    ctx.addReusableCloseStatement(s"")
  }

  private[flink] def prepareTermForAggMapIteration(
      ctx: CodeGeneratorContext,
      outputTerm: String,
      outputType: RowType,
      aggMapKeyType: RowType,
      aggBufferType: RowType,
      outputClass: Class[_ <: BaseRow]): (String, String, String) = {
    // prepare iteration var terms
    val reuseAggMapKeyTerm = CodeGenUtils.newName("reuseAggMapKey")
    val reuseAggBufferTerm = CodeGenUtils.newName("reuseAggBuffer")
    val reuseAggMapEntryTerm = CodeGenUtils.newName("reuseAggMapEntry")
    // gen code to prepare agg output using agg buffer and key from the aggregate map
    val binaryRow = classOf[BinaryRow].getName
    val mapEntryTypeTerm = classOf[BytesHashMap.Entry].getCanonicalName

    ctx.addReusableOutputRecord(outputType, outputClass, outputTerm)
    ctx.addReusableMember(
      s"private transient $binaryRow $reuseAggMapKeyTerm = " +
          s"new $binaryRow(${aggMapKeyType.getFieldCount});")
    ctx.addReusableMember(
      s"private transient $binaryRow $reuseAggBufferTerm = " +
          s"new $binaryRow(${aggBufferType.getFieldCount});")
    ctx.addReusableMember(
      s"private transient $mapEntryTypeTerm $reuseAggMapEntryTerm = " +
          s"new $mapEntryTypeTerm($reuseAggMapKeyTerm, $reuseAggBufferTerm);"
    )
    (reuseAggMapEntryTerm, reuseAggMapKeyTerm, reuseAggBufferTerm)
  }

  private[flink] def genHashAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      groupingAndAuxGrouping: (Array[Int], Array[Int]),
      inputTerm: String,
      inputType: RowType,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggArgs: Array[Array[Int]],
      aggregates: Seq[UserDefinedFunction],
      currentAggBufferTerm: String,
      aggBufferRowType: RowType,
      aggBufferTypes: Array[Array[LogicalType]],
      outputTerm: String,
      outputType: RowType,
      groupKeyTerm: String,
      aggBufferTerm: String): (GeneratedExpression, GeneratedExpression, GeneratedExpression) = {
    val (grouping, auxGrouping) = groupingAndAuxGrouping
    // build mapping for DeclarativeAggregationFunction binding references
    val argsMapping = buildAggregateArgsMapping(
      isMerge, grouping.length, inputType, auxGrouping, aggArgs, aggBufferTypes)
    val aggBuffMapping = buildAggregateAggBuffMapping(aggBufferTypes)
    // gen code to create empty agg buffer
    val initedAggBuffer = genReusableEmptyAggBuffer(
      ctx, builder, inputTerm, inputType, auxGrouping, aggregates, aggBufferRowType)
    if (auxGrouping.isEmpty) {
      // create an empty agg buffer and initialized make it reusable
      ctx.addReusableOpenStatement(initedAggBuffer.code)
    }
    // gen code to update agg buffer from the aggregate map
    val aggregate = genAggregate(
      isMerge,
      ctx,
      builder,
      inputType,
      inputTerm,
      auxGrouping,
      aggregates,
      aggCallToAggFunction,
      argsMapping,
      aggBuffMapping,
      currentAggBufferTerm,
      aggBufferRowType)

    val outputExpr = genHashAggOutputExpr(
      isMerge,
      isFinal,
      ctx,
      builder,
      auxGrouping,
      aggregates,
      argsMapping,
      aggBuffMapping,
      outputTerm,
      outputType,
      inputTerm,
      inputType,
      Some(groupKeyTerm),
      aggBufferTerm,
      aggBufferRowType)

    (initedAggBuffer, aggregate, outputExpr)
  }

  private[flink] def buildAggregateAggBuffMapping(
      aggBufferTypes: Array[Array[LogicalType]]): Array[Array[(Int, LogicalType)]] = {
    var aggBuffOffset = 0
    val mapping = for (aggIndex <- aggBufferTypes.indices) yield {
      val types = aggBufferTypes(aggIndex)
      val indexes = (aggBuffOffset until aggBuffOffset + types.length).toArray
      aggBuffOffset += types.length
      indexes.zip(types)
    }
    mapping.toArray
  }

  /**
    * Generate codes which will init the empty agg buffer.
    */
  private[flink] def genReusableEmptyAggBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm, inputFieldMapping = Some(auxGrouping))

    val initAuxGroupingExprs = auxGrouping.map { idx =>
      GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm, idx)
    }

    val initAggCallBufferExprs = aggregates.flatMap(a =>
      a.asInstanceOf[DeclarativeAggregateFunction].initialValuesExpressions)
        .map(_.accept(new RexNodeConverter(builder)))
        .map(exprCodegen.generateExpression)

    val initAggBufferExprs = initAuxGroupingExprs ++ initAggCallBufferExprs

    // empty agg buffer and writer will be reused
    val emptyAggBufferTerm = CodeGenUtils.newName("emptyAggBuffer")
    val emptyAggBufferWriterTerm = CodeGenUtils.newName("emptyAggBufferWriterTerm")
    exprCodegen.generateResultExpression(
      initAggBufferExprs,
      aggBufferType,
      classOf[BinaryRow],
      emptyAggBufferTerm,
      Some(emptyAggBufferWriterTerm)
    )
  }

  private[flink] def genAggregate(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputType: RowType,
      inputTerm: String,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]],
      currentAggBufferTerm: String,
      aggBufferRowType: RowType): GeneratedExpression = {
    if (isMerge) {
      genMergeAggBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        currentAggBufferTerm,
        auxGrouping,
        aggregates,
        argsMapping,
        aggBuffMapping,
        aggBufferRowType)
    } else {
      genAccumulateAggBuffer(
        ctx,
        builder,
        inputTerm,
        inputType,
        currentAggBufferTerm,
        auxGrouping,
        aggCallToAggFunction,
        argsMapping,
        aggBuffMapping,
        aggBufferRowType)
    }
  }

  private[flink] def genHashAggOutputExpr(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]],
      outputTerm: String,
      outputType: RowType,
      inputTerm: String,
      inputType: RowType,
      groupKeyTerm: Option[String],
      aggBufferTerm: String,
      aggBufferType: RowType): GeneratedExpression = {
    // gen code to get agg result
    val exprCodegen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType, inputTerm = aggBufferTerm)
    val resultExpr = if (isFinal) {
      val bindRefOffset = inputType.getFieldCount
      val getAuxGroupingExprs = auxGrouping.indices.map { idx =>
        val (_, resultType) = aggBuffMapping(idx)(0)
        new ResolvedAggInputReference("aux_group", bindRefOffset + idx, resultType)
      }.map(_.accept(new RexNodeConverter(builder))).map(exprCodegen.generateExpression)

      val getAggValueExprs = aggregates.zipWithIndex.map {
        case (agg: DeclarativeAggregateFunction, aggIndex) =>
          val idx = auxGrouping.length + aggIndex
          agg.getValueExpression.accept(
            ResolveReference(ctx, isMerge, bindRefOffset, agg, idx, argsMapping, aggBuffMapping))
      }.map(_.accept(new RexNodeConverter(builder))).map(exprCodegen.generateExpression)

      val getValueExprs = getAuxGroupingExprs ++ getAggValueExprs
      val aggValueTerm = CodeGenUtils.newName("aggVal")
      val valueType = RowType.of(getValueExprs.map(_.resultType): _*)
      exprCodegen.generateResultExpression(
        getValueExprs,
        valueType,
        classOf[GenericRow],
        aggValueTerm)
    } else {
      new GeneratedExpression(aggBufferTerm, "false", "", aggBufferType)
    }
    // add grouping keys if exists
    groupKeyTerm match {
      case Some(key) =>
        val output =
          s"""
             |${resultExpr.code}
             |$outputTerm.replace($key, ${resultExpr.resultTerm});
         """.stripMargin
        new GeneratedExpression(outputTerm, "false", output, outputType)
      case _ => resultExpr
    }
  }

  /**
    * Resolves the given expression to a resolved Expression.
    *
    * @param isMerge this is called from merge() method
    */
  private case class ResolveReference(
      ctx: CodeGeneratorContext,
      isMerge: Boolean,
      offset: Int,
      agg: DeclarativeAggregateFunction,
      aggIndex: Int,
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]]) extends ExpressionVisitor[Expression] {

    override def visit(call: CallExpression): Expression = ???

    override def visit(valueLiteralExpression: ValueLiteralExpression): Expression = {
      valueLiteralExpression
    }

    override def visit(input: FieldReferenceExpression): Expression = {
      input
    }

    override def visit(typeLiteral: TypeLiteralExpression): Expression = {
      typeLiteral
    }

    private def visitUnresolvedCallExpression(
        unresolvedCall: UnresolvedCallExpression): Expression = {
      ApiExpressionUtils.unresolvedCall(
        unresolvedCall.getFunctionDefinition,
        unresolvedCall.getChildren.map(_.accept(this)): _*)
    }

    private def visitUnresolvedFieldReference(
        input: UnresolvedReferenceExpression): Expression = {
      agg.aggBufferAttributes.indexOf(input) match {
        case -1 =>
          // We always use UnresolvedFieldReference to represent reference of input field.
          // In non-merge case, the input is operand of the aggregate function. But in merge
          // case, the input is aggregate buffers which sent by local aggregate.
          val localIndex =
            if (isMerge) agg.mergeOperands.indexOf(input) else agg.operands.indexOf(input)
          val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
          new ResolvedAggInputReference(input.getName, inputIndex, inputType)
        case localIndex =>
          val (aggBuffAttrIndex, aggBuffAttrType) = aggBuffMapping(aggIndex)(localIndex)
          new ResolvedAggInputReference(
            input.getName, offset + aggBuffAttrIndex, aggBuffAttrType)
      }
    }

    override def visit(other: Expression): Expression = {
      other match {
        case u : UnresolvedReferenceExpression => visitUnresolvedFieldReference(u)
        case u : UnresolvedCallExpression => visitUnresolvedCallExpression(u)
        case _ => other
      }
    }
  }

  /**
    * Generate codes which will read input,
    * merge aggregate buffers and update the aggregation map
    */
  private[flink] def genMergeAggBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      currentAggBufferTerm: String,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType, inputTerm = currentAggBufferTerm)

    val mergeExprs = aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val bindRefOffset = inputType.getFieldCount
        agg.mergeExpressions.map(
          _.accept(ResolveReference(
            ctx, isMerge = true, bindRefOffset, agg, idx, argsMapping, aggBuffMapping)))
    }.map(_.accept(new RexNodeConverter(builder))).map(exprCodegen.generateExpression)

    val aggBufferTypeWithoutAuxGrouping = if (auxGrouping.nonEmpty) {
      // auxGrouping does not need merge-code
      RowType.of(
        aggBufferType.getChildren.slice(auxGrouping.length, aggBufferType.getFieldCount)
            .toArray[LogicalType],
        aggBufferType.getFieldNames.slice(auxGrouping.length, aggBufferType.getFieldCount)
            .toArray[String])
    } else {
      aggBufferType
    }

    val mergeExprIdxToOutputRowPosMap = mergeExprs.indices.map{
      i => i -> (i + auxGrouping.length)
    }.toMap

    // update agg buff in-place
    exprCodegen.generateResultExpression(
      mergeExprs,
      mergeExprIdxToOutputRowPosMap,
      aggBufferTypeWithoutAuxGrouping,
      classOf[BinaryRow],
      outRow = currentAggBufferTerm,
      outRowWriter = None,
      reusedOutRow = true,
      outRowAlreadyExists = true
    )
  }

  /**
    * Generate codes which will read input,
    * accumulating aggregate buffers and updating the aggregation map
    */
  private[flink] def genAccumulateAggBuffer(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      currentAggBufferTerm: String,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType, inputTerm = currentAggBufferTerm)

    val accumulateExprsWithFilterArgs = aggCallToAggFunction.zipWithIndex.flatMap {
      case (aggCallToAggFun, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val bindRefOffset = inputType.getFieldCount
        val aggCall = aggCallToAggFun._1
        aggCallToAggFun._2 match {
          case agg: DeclarativeAggregateFunction =>
            agg.accumulateExpressions.map(
              _.accept(ResolveReference(
                ctx, isMerge = false, bindRefOffset, agg, idx, argsMapping, aggBuffMapping))
            ).map(e => (e, aggCall))
        }
    }.map {
      case (expr: Expression, aggCall: AggregateCall) =>
        (exprCodegen.generateExpression(expr.accept(new RexNodeConverter(builder))),
            aggCall.filterArg)
    }

    // update agg buff in-place
    val code = accumulateExprsWithFilterArgs.zipWithIndex.map({
      case ((accumulateExpr, filterArg), index) =>
        val idx = auxGrouping.length + index
        val t = aggBufferType.getTypeAt(idx)
        val writeCode = binaryRowFieldSetAccess(
          idx, currentAggBufferTerm, t, accumulateExpr.resultTerm)
        val innerCode =
          s"""
             |${accumulateExpr.code}
             |if (${accumulateExpr.nullTerm}) {
             |  ${binaryRowSetNull(idx, currentAggBufferTerm, t)};
             |} else {
             |  $writeCode;
             |}
             |""".stripMargin.trim

        if (filterArg >= 0) {
          s"""
             |if ($inputTerm.getBoolean($filterArg)) {
             | $innerCode
             |}
          """.stripMargin
        } else {
          innerCode
        }

    }) mkString "\n"

    GeneratedExpression(currentAggBufferTerm, "false", code, aggBufferType)
  }

  /**
    * Generate codes which will read aggregation map,
    * get the aggregate values
    */
  private[flink] def genAggMapIterationAndOutput(
      ctx: CodeGeneratorContext,
      isFinal: Boolean,
      aggregateMapTerm: String,
      reuseAggMapEntryTerm: String,
      reuseAggBufferTerm: String,
      outputExpr: GeneratedExpression): String = {
    // gen code to iterating the aggregate map and output to downstream
    val inputUnboxingCode =
      if (isFinal) s"${ctx.reuseInputUnboxingCode(reuseAggBufferTerm)}" else ""

    val iteratorTerm = CodeGenUtils.newName("iterator")
    val mapEntryTypeTerm = classOf[BytesHashMap.Entry].getCanonicalName
    s"""
       |org.apache.flink.util.MutableObjectIterator<$mapEntryTypeTerm> $iteratorTerm =
       |  $aggregateMapTerm.getEntryIterator();
       |while ($iteratorTerm.next($reuseAggMapEntryTerm) != null) {
       |   // set result and output
       |   $inputUnboxingCode
       |   ${outputExpr.code}
       |   ${OperatorCodeGenerator.generateCollect(outputExpr.resultTerm)}
       |}
       """.stripMargin
  }

  private[flink] def genRetryAppendToMap(
      aggregateMapTerm: String,
      currentKeyTerm: String,
      initedAggBuffer: GeneratedExpression,
      lookupInfo: String,
      currentAggBufferTerm: String): String = {
    s"""
       | // reset aggregate map retry append
       |$aggregateMapTerm.reset();
       |$lookupInfo = $aggregateMapTerm.lookup($currentKeyTerm);
       |try {
       |  $currentAggBufferTerm =
       |    $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
       |} catch (java.io.EOFException e) {
       |  throw new OutOfMemoryError("BytesHashMap Out of Memory.");
       |}
       """.stripMargin
  }

  private[flink] def genAggMapOOMHandling(
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      groupingAndAuxGrouping: (Array[Int], Array[Int]),
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggArgs: Array[Array[Int]],
      aggResultTypes: Seq[DataType],
      udaggs: Map[AggregateFunction[_, _], String],
      logTerm: String,
      aggregateMapTerm: String,
      aggMapKVTypesTerm: (String, String),
      aggMapKVRowType: (RowType, RowType),
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]],
      outputTerm: String,
      outputType: RowType,
      outputResultFromMap: String,
      sorterTerm: String,
      retryAppend: String): (String, String) = {
    val (grouping, auxGrouping) = groupingAndAuxGrouping
    if (isFinal) {
      val logMapSpilling =
        CodeGenUtils.genLogInfo(
          logTerm, s"BytesHashMap out of memory with {} entries, start spilling.",
          s"$aggregateMapTerm.getNumElements()")

      // gen fallback to sort agg
      val (groupKeyTypesTerm, aggBufferTypesTerm) = aggMapKVTypesTerm
      val (groupKeyRowType, aggBufferRowType) =  aggMapKVRowType
      prepareFallbackSorter(ctx, sorterTerm)
      val createSorter = genCreateFallbackSorter(
        ctx, groupKeyRowType, groupKeyTypesTerm, aggBufferTypesTerm, sorterTerm)
      val fallbackToSortAggCode = genFallbackToSortAgg(
        ctx,
        builder,
        grouping,
        auxGrouping,
        aggCallToAggFunction,
        aggArgs,
        aggCallToAggFunction.map(_._2),
        aggResultTypes,
        udaggs,
        aggregateMapTerm,
        (groupKeyRowType, aggBufferRowType),
        aggregateMapTerm,
        sorterTerm,
        outputTerm,
        outputType,
        aggBufferNames,
        aggBufferTypes)

      val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
      val dealWithAggHashMapOOM =
        s"""
           |$logMapSpilling
           | // hash map out of memory, spill to external sorter
           |if ($sorterTerm == null) {
           |  $createSorter
           |}
           | // sort and spill
           |$sorterTerm.sortAndSpill(
           |  $aggregateMapTerm.getRecordAreaMemorySegments(),
           |  $aggregateMapTerm.getNumElements(),
           |  new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments()));
           | // retry append
           |$retryAppend
       """.stripMargin
      (dealWithAggHashMapOOM, fallbackToSortAggCode)
    } else {
      val logMapOutput =
        CodeGenUtils.genLogInfo(
          logTerm, s"BytesHashMap out of memory with {} entries, output directly.",
          s"$aggregateMapTerm.getNumElements()")

      val dealWithAggHashMapOOM =
        s"""
           |$logMapOutput
           | // hash map out of memory, output directly
           |$outputResultFromMap
           | // retry append
           |$retryAppend
          """.stripMargin
      (dealWithAggHashMapOOM, "")
    }
  }

  private[flink] def prepareFallbackSorter(ctx: CodeGeneratorContext, sorterTerm: String): Unit = {
    val sorterTypeTerm = classOf[BufferedKVExternalSorter].getName
    ctx.addReusableMember(s"transient $sorterTypeTerm $sorterTerm;")
    ctx.addReusableCloseStatement(s"if ($sorterTerm != null) $sorterTerm.close();")
  }

  private[flink] def prepareMetrics(
      ctx: CodeGeneratorContext, hashTerm: String, sorterTerm: String): Unit = {
    val gauge = classOf[Gauge[_]].getCanonicalName
    val longType = classOf[java.lang.Long].getCanonicalName

    val numSpillFiles =
      s"""
         |getMetricGroup().gauge("numSpillFiles", new $gauge<$longType>() {
         | @Override
         | public $longType getValue() {
         |  return $hashTerm.getNumSpillFiles();
         |  }
         | });
       """.stripMargin.trim

    val memoryUsedSizeInBytes =
      s"""
         |getMetricGroup().gauge("memoryUsedSizeInBytes", new $gauge<$longType>() {
         | @Override
         | public $longType getValue() {
         |  return $hashTerm.getUsedMemoryInBytes();
         |  }
         | });
       """.stripMargin.trim
    ctx.addReusableOpenStatement(numSpillFiles)
    ctx.addReusableOpenStatement(memoryUsedSizeInBytes)

    if (sorterTerm != null) {
      val spillInBytes =
        s"""
           | getMetricGroup().gauge("spillInBytes", new $gauge<$longType>() {
           |  @Override
           |  public $longType getValue() {
           |    return $hashTerm.getSpillInBytes();
           |   }
           |});
       """.stripMargin.trim
      ctx.addReusableOpenStatement(spillInBytes)
    }
  }

  private[flink] def genCreateFallbackSorter(
      ctx: CodeGeneratorContext,
      groupKeyRowType: RowType,
      groupKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      sorterTerm: String): String = {
    val keyComputerTerm = CodeGenUtils.newName("keyComputer")
    val recordComparatorTerm = CodeGenUtils.newName("recordComparator")
    val prepareSorterCode = genKVSorterPrepareCode(
      ctx, keyComputerTerm, recordComparatorTerm, groupKeyRowType)

    val binaryRowSerializerTypeTerm = classOf[BinaryRowSerializer].getName
    val sorterTypeTerm = classOf[BufferedKVExternalSorter].getName
    s"""
       |  $prepareSorterCode
       |  $sorterTerm = new $sorterTypeTerm(
       |    getContainingTask().getEnvironment().getIOManager(),
       |    new $binaryRowSerializerTypeTerm($groupKeyTypesTerm.length),
       |    new $binaryRowSerializerTypeTerm($aggBufferTypesTerm.length),
       |    $keyComputerTerm, $recordComparatorTerm,
       |    getContainingTask().getEnvironment().getMemoryManager().getPageSize(),
       |    getContainingTask().getJobConfiguration()
       |  );
       """.stripMargin
  }

  private[flink] def genFallbackToSortAgg(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggArgs: Array[Array[Int]],
      aggregates: Seq[UserDefinedFunction],
      aggResultTypes: Seq[DataType],
      udaggs: Map[AggregateFunction[_, _], String],
      mapTerm: String,
      mapKVRowTypes: (RowType, RowType),
      aggregateMapTerm: String,
      sorterTerm: String,
      outputTerm: String,
      outputType: RowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[LogicalType]]): String = {
    val (groupKeyRowType, aggBufferRowType) = mapKVRowTypes
    val keyTerm = CodeGenUtils.newName("key")
    val lastKeyTerm = CodeGenUtils.newName("lastKey")
    val keyNotEquals = AggCodeGenHelper.genGroupKeyChangedCheckCode(keyTerm, lastKeyTerm)

    val joinedRow = classOf[JoinedRow].getName
    val fallbackInputTerm = ctx.addReusableLocalVariable(joinedRow, "fallbackInput")
    val fallbackInputType = RowType.of(
      (groupKeyRowType.getChildren ++ aggBufferRowType.getChildren).toArray,
      (groupKeyRowType.getFieldNames ++ aggBufferRowType.getFieldNames).toArray)

    val (initAggBufferCode, updateAggBufferCode, resultExpr) = AggCodeGenHelper.genSortAggCodes(
      isMerge = true,
      isFinal = true,
      ctx,
      builder,
      grouping,
      auxGrouping,
      aggCallToAggFunction,
      aggArgs,
      aggregates,
      aggResultTypes,
      udaggs,
      fallbackInputTerm,
      fallbackInputType,
      aggBufferNames,
      aggBufferTypes,
      outputType,
      forHashAgg = true)

    val kvPairTerm = CodeGenUtils.newName("kvPair")
    val kvPairTypeTerm = classOf[JTuple2[BinaryRow, BinaryRow]].getName
    val aggBuffTerm = CodeGenUtils.newName("val")
    val binaryRow = classOf[BinaryRow].getName

    s"""
       |  $binaryRow $lastKeyTerm = null;
       |  $kvPairTypeTerm<$binaryRow, $binaryRow> $kvPairTerm = null;
       |  $binaryRow $keyTerm = null;
       |  $binaryRow $aggBuffTerm = null;
       |  $fallbackInputTerm = new $joinedRow();
       |
       |  // free hash map memory, but not release back to memory manager
       |
       |  org.apache.flink.util.MutableObjectIterator<$kvPairTypeTerm<$binaryRow, $binaryRow>>
       |    iterator = $sorterTerm.getKVIterator();
       |
       |  while (
       |    ($kvPairTerm = ($kvPairTypeTerm<$binaryRow, $binaryRow>) iterator.next()) != null) {
       |    $keyTerm = ($binaryRow) $kvPairTerm.f0;
       |    $aggBuffTerm = ($binaryRow) $kvPairTerm.f1;
       |    // prepare input
       |    $fallbackInputTerm.replace($keyTerm, $aggBuffTerm);
       |    if ($lastKeyTerm == null) {
       |      // found first key group
       |      $lastKeyTerm = $keyTerm.copy();
       |      $initAggBufferCode
       |    } else if ($keyNotEquals) {
       |      // output current group aggregate result
       |      ${resultExpr.code}
       |      $outputTerm.replace($lastKeyTerm, ${resultExpr.resultTerm});
       |      ${OperatorCodeGenerator.generateCollect(outputTerm)}
       |      // found new group
       |      $lastKeyTerm = $keyTerm.copy();
       |      $initAggBufferCode
       |    }
       |    // reusable field access codes for agg buffer merge
       |    ${ctx.reuseInputUnboxingCode(fallbackInputTerm)}
       |    // merge aggregate map's value into aggregate buffer fields
       |    $updateAggBufferCode
       |  }
       |
       |  // output last key group aggregate result
       |  ${resultExpr.code}
       |  $outputTerm.replace($lastKeyTerm, ${resultExpr.resultTerm});
       |  ${OperatorCodeGenerator.generateCollect(outputTerm)}
       """.stripMargin
  }

  private[flink] def genKVSorterPrepareCode(
      ctx: CodeGeneratorContext,
      keyComputerTerm: String,
      recordComparatorTerm: String,
      aggMapKeyType: RowType) : String = {
    val keyFieldTypes = aggMapKeyType.getChildren.toArray(Array[LogicalType]())
    val keys = keyFieldTypes.indices.toArray
    val orders = keys.map((_) => true)
    val nullsIsLast = SortUtil.getNullDefaultOrders(orders)

    val sortCodeGenerator = new SortCodeGenerator(
      ctx.tableConfig, keys, keyFieldTypes, orders, nullsIsLast)
    val computer = sortCodeGenerator.generateNormalizedKeyComputer("AggMapKeyComputer")
    val comparator = sortCodeGenerator.generateRecordComparator("AggMapValueComparator")

    val keyComputerTypeTerm = classOf[NormalizedKeyComputer].getName
    val keyComputeInnerClassTerm = computer.getClassName
    val recordComparatorTypeTerm = classOf[RecordComparator].getName
    val recordComparatorInnerClassTerm = comparator.getClassName
    ctx.addReusableInnerClass(keyComputeInnerClassTerm, computer.getCode)
    ctx.addReusableInnerClass(recordComparatorInnerClassTerm, comparator.getCode)

    val computerRefs = ctx.addReusableObject(computer.getReferences, "computerRefs")
    val comparatorRefs = ctx.addReusableObject(comparator.getReferences, "comparatorRefs")

    s"""
       |  $keyComputerTypeTerm $keyComputerTerm = new $keyComputeInnerClassTerm($computerRefs);
       |  $recordComparatorTypeTerm $recordComparatorTerm =
       |    new $recordComparatorInnerClassTerm($comparatorRefs);
       |""".stripMargin
  }
}
