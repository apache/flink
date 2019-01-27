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

package org.apache.flink.table.codegen.agg

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.metrics.Gauge
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.functions.{AggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.codegen.CodeGenUtils.{binaryRowFieldSetAccess, binaryRowSetNull}
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.sort.{BufferedKVExternalSorter, NormalizedKeyComputer, RecordComparator}
import org.apache.flink.table.runtime.util.{BytesHashMap, BytesHashMapSpillMemorySegmentPool}
import org.apache.flink.table.typeutils.{BinaryRowSerializer, TypeUtils}
import org.apache.flink.table.util.NodeResourceUtil

trait BatchExecHashAggregateCodeGen extends BatchExecAggregateCodeGen {

  private[flink] def prepareHashAggKVTypes(
      ctx: CodeGeneratorContext,
      aggMapKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      aggMapKeyType: RowType,
      aggBufferType: RowType): Unit = {
    val tpTerm = classOf[InternalType].getName
    ctx.addReusableMember(
      s"private transient $tpTerm[] $aggMapKeyTypesTerm;",
      s"$aggMapKeyTypesTerm = ${ctx.addReferenceObj(
        aggMapKeyType.getFieldInternalTypes, s"$tpTerm[]")};")
    ctx.addReusableMember(
      s"private transient $tpTerm[] $aggBufferTypesTerm;",
      s"$aggBufferTypesTerm = ${ctx.addReferenceObj(
        aggBufferType.getFieldInternalTypes, s"$tpTerm[]")};")
  }

  private[flink] def prepareHashAggMap(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      reservedManagedMemory: Long,
      maxManagedMemory: Long,
      groupKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      aggregateMapTerm: String): Unit = {
    // allocate memory segments for aggregate map

    // create aggregate map
    val mapTypeTerm = classOf[BytesHashMap].getName
    val perRequestSize = NodeResourceUtil.getPerRequestManagedMemory(config.getConf) *
        NodeResourceUtil.SIZE_IN_MB
    ctx.addReusableMember(s"private transient $mapTypeTerm $aggregateMapTerm;")
    ctx.addReusableOpenStatement(s"$aggregateMapTerm " +
        s"= new $mapTypeTerm(" +
        s"this.getContainingTask()," +
        s"this.getContainingTask().getEnvironment().getMemoryManager()," +
        s"${reservedManagedMemory}L," +
        s"${maxManagedMemory}L," +
        s"${perRequestSize}L," +
        s" $groupKeyTypesTerm," +
        s" $aggBufferTypesTerm);")
    // close aggregate map and release memory segments
    ctx.addReusableCloseStatement(s"$aggregateMapTerm.free();")
    ctx.addReusableCloseStatement(s"")
  }

  def getOutputRowClass: Class[_ <: BaseRow]

  private[flink] def prepareTermForAggMapIteration(
      ctx: CodeGeneratorContext,
      outputTerm: String,
      outputType: RowType,
      aggMapKeyType: RowType,
      aggBufferType: RowType): (String, String, String) = {
    // prepare iteration var terms
    val reuseAggMapKeyTerm = CodeGenUtils.newName("reuseAggMapKey")
    val reuseAggBufferTerm = CodeGenUtils.newName("reuseAggBuffer")
    val reuseAggMapEntryTerm = CodeGenUtils.newName("reuseAggMapEntry")
    // gen code to prepare agg output using agg buffer and key from the aggregate map
    val binaryRow = classOf[BinaryRow].getName
    val mapEntryTypeTerm = classOf[BytesHashMap.Entry].getCanonicalName

    ctx.addOutputRecord(outputType, getOutputRowClass, outputTerm)
    ctx.addReusableMember(
      s"private transient $binaryRow $reuseAggMapKeyTerm = " +
          s"new $binaryRow(${aggMapKeyType.getArity});")
    ctx.addReusableMember(
      s"private transient $binaryRow $reuseAggBufferTerm = " +
          s"new $binaryRow(${aggBufferType.getArity});")
    ctx.addReusableMember(
      s"private transient $mapEntryTypeTerm $reuseAggMapEntryTerm = " +
          s"new $mapEntryTypeTerm($reuseAggMapKeyTerm, $reuseAggBufferTerm);"
    )
    (reuseAggMapEntryTerm, reuseAggMapKeyTerm, reuseAggBufferTerm)
  }

  /**
    * Generate codes which will read aggregation map,
    * get the aggregate values
    */
  private[flink] def genAggMapIterationAndOutput(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      isFinal: Boolean,
      aggregateMapTerm: String,
      reuseAggMapEntryTerm: String,
      reuseAggBufferTerm: String,
      outputExpr: GeneratedExpression): String = {
    // gen code to iterating the aggregate map and output to downstream
    val inputUnboxingCode =
      if (isFinal) s"${ctx.reuseInputUnboxingCode(Set(reuseAggBufferTerm))}" else ""

    val iteratorTerm = CodeGenUtils.newName("iterator")
    val mapEntryTypeTerm = classOf[BytesHashMap.Entry].getCanonicalName
    s"""
       |org.apache.flink.util.MutableObjectIterator<$mapEntryTypeTerm> $iteratorTerm =
       |  $aggregateMapTerm.getEntryIterator();
       |while ($iteratorTerm.next($reuseAggMapEntryTerm) != null) {
       |   // set result and output
       |   $inputUnboxingCode
       |   ${outputExpr.code}
       |   ${OperatorCodeGenerator.generatorCollect(outputExpr.resultTerm)}
       |}
       """.stripMargin
  }

  // ===============================================================================================

  /**
    * In the cases of hash aggregation,
    * we store the aggregate buffer as BytesHashMap's value in the form of BinaryRow.
    * We use an index to locate the aggregate buffer field.
    */
  private[flink] def bindReference(
      isMerge: Boolean,
      offset: Int,
      agg: DeclarativeAggregateFunction,
      aggIndex: Int,
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]])
      : PartialFunction[Expression, Expression] = {
    case input: UnresolvedFieldReference =>
      // We always use UnresolvedFieldReference to represent reference of input field.
      // In non-merge case, the input is operand of the aggregate function. But in merge
      // case, the input is aggregate buffers which sent by local aggregate.
      val localIndex = if (isMerge) agg.inputAggBufferAttributes.indexOf(input)
      else agg.operands.indexOf(input)
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      ResolvedAggInputReference(input.name, inputIndex, inputType)
    case aggBuffAttr: UnresolvedAggBufferReference =>
      val localIndex = agg.aggBufferAttributes.indexOf(aggBuffAttr)
      val (aggBuffAttrIndex, aggBuffAttrType) = aggBuffMapping(aggIndex)(localIndex)
      ResolvedAggInputReference(
        aggBuffAttr.name, offset + aggBuffAttrIndex, aggBuffAttrType)
  }

  /**
    * Generate codes which will read input,
    * accumulating aggregate buffers and updating the aggregation map
    */
  private[flink] def genAccumulateAggBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputRelDataType: RelDataType,
      inputTerm: String,
      inputType: RowType,
      currentAggBufferTerm: String,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType, inputTerm = currentAggBufferTerm)

    val accumulateExprsWithFilterArgs = aggCallToAggFunction.zipWithIndex.flatMap {
      case (aggCallToAggFun, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val bindRefOffset = inputRelDataType.getFieldCount
        val aggCall = aggCallToAggFun._1
        aggCallToAggFun._2 match {
          case agg: DeclarativeAggregateFunction =>
            agg.accumulateExpressions.map(
              _.postOrderTransform(bindReference(
                isMerge = false, bindRefOffset, agg, idx, argsMapping, aggBuffMapping))
            ).map(e => (e, aggCall))
        }
    }.map {
      case (expr: Expression, aggCall: AggregateCall) =>
        (exprCodegen.generateExpression(expr.toRexNode(builder)), aggCall.filterArg)
    }

    // update agg buff in-place
    val code = accumulateExprsWithFilterArgs.zipWithIndex.map({
      case ((accumulateExpr, filterArg), index) =>
        val idx = auxGrouping.length + index
        val t = aggBufferType.getInternalTypeAt(idx)
        val writeCode = binaryRowFieldSetAccess(
          idx, currentAggBufferTerm, t.toInternalType, accumulateExpr.resultTerm)
        val innerCode = if (config.getNullCheck) {
          s"""
             |${accumulateExpr.code}
             |if (${accumulateExpr.nullTerm}) {
             |  ${binaryRowSetNull(idx, currentAggBufferTerm, t.toInternalType)};
             |} else {
             |  $writeCode;
             |}
             |""".stripMargin.trim
        }
        else {
          s"""
             |${accumulateExpr.code}
             |$writeCode;
             |""".stripMargin.trim
        }

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

    GeneratedExpression(currentAggBufferTerm, "false", code, aggBufferType.toInternalType)
  }

  /**
    * Generate codes which will init the empty agg buffer.
    */
  private[flink] def genReusableEmptyAggBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputTerm: String,
      inputType: RowType,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(inputType, inputTerm = inputTerm, inputFieldMapping = Some(auxGrouping))

    val initAuxGroupingExprs = auxGrouping.map { idx =>
      CodeGenUtils.generateFieldAccess(
        ctx, inputType.toInternalType, inputTerm, idx, nullCheck = true)
    }

    val initAggCallBufferExprs = aggregates.flatMap(a =>
      a.asInstanceOf[DeclarativeAggregateFunction].initialValuesExpressions)
        .map(_.toRexNode(builder))
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

  /**
    * Generate codes which will read input,
    * merge aggregate buffers and update the aggregation map
    */
  private[flink] def genMergeAggBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputRelDataType: RelDataType,
      inputTerm: String,
      inputType: RowType,
      currentAggBufferTerm: String,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]],
      aggBufferType: RowType): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType.toInternalType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType.toInternalType, inputTerm = currentAggBufferTerm)

    val mergeExprs = aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val bindRefOffset = inputRelDataType.getFieldCount
        agg.mergeExpressions.map(
          _.postOrderTransform(bindReference(
            isMerge = true, bindRefOffset, agg, idx, argsMapping, aggBuffMapping)))
    }.map(_.toRexNode(builder)).map(exprCodegen.generateExpression)

    val aggBufferTypeWithoutAuxGrouping = if (auxGrouping.nonEmpty) {
      // auxGrouping does not need merge-code
      new RowType(
        aggBufferType.getFieldTypes.slice(auxGrouping.length, aggBufferType.getArity),
        aggBufferType.getFieldNames.slice(auxGrouping.length, aggBufferType.getArity))
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

  private[flink] def genAggregate(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputRelDataType: RelDataType,
      inputType: RowType,
      inputTerm: String,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]],
      currentAggBufferTerm: String,
      aggBufferRowType: RowType): GeneratedExpression = {
    if (isMerge) {
      genMergeAggBuffer(ctx, config, builder, inputRelDataType, inputTerm, inputType,
        currentAggBufferTerm, auxGrouping, aggregates, argsMapping, aggBuffMapping,
        aggBufferRowType)
    } else {
      genAccumulateAggBuffer(ctx, config, builder, inputRelDataType, inputTerm, inputType,
        currentAggBufferTerm, auxGrouping, aggCallToAggFunction, argsMapping, aggBuffMapping,
        aggBufferRowType)
    }
  }

  private[flink] def genHashAggOutputExpr(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputRelDataType: RelDataType,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]],
      outputTerm: String,
      outputType: RowType,
      inputTerm: String,
      inputType: RowType,
      groupKeyTerm: Option[String],
      aggBufferTerm: String,
      aggBufferType: RowType): GeneratedExpression = {
    // gen code to get agg result
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType.toInternalType, inputTerm = inputTerm)
        .bindSecondInput(aggBufferType.toInternalType, inputTerm = aggBufferTerm)
    val resultExpr = if (isFinal) {
      val bindRefOffset = inputRelDataType.getFieldCount
      val getAuxGroupingExprs = auxGrouping.indices.map { idx =>
        val (_, resultType) = aggBuffMapping(idx)(0)
        ResolvedAggInputReference("aux_group", bindRefOffset + idx, resultType)
      }.map(_.toRexNode(builder)).map(exprCodegen.generateExpression)

      val getAggValueExprs = aggregates.zipWithIndex.map {
        case (agg: DeclarativeAggregateFunction, aggIndex) =>
          val idx = auxGrouping.length + aggIndex
          agg.getValueExpression.postOrderTransform(
            bindReference(isMerge, bindRefOffset, agg, idx, argsMapping, aggBuffMapping))
      }.map(_.toRexNode(builder)).map(exprCodegen.generateExpression)

      val getValueExprs = getAuxGroupingExprs ++ getAggValueExprs
      val aggValueTerm = CodeGenUtils.newName("aggVal")
      val valueType = new RowType(getValueExprs.map(_.resultType): _*)
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

  private[flink] def genHashAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      groupingAndAuxGrouping: (Array[Int], Array[Int]),
      inputRelDataType: RelDataType,
      inputTerm: String,
      inputType: RowType,
      aggregateCalls: Seq[AggregateCall],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      currentAggBufferTerm: String,
      aggBufferRowType: RowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      outputTerm: String,
      outputType: RowType,
      groupKeyTerm: String,
      aggBufferTerm: String): (GeneratedExpression, GeneratedExpression, GeneratedExpression) = {

    val (grouping, auxGrouping) = groupingAndAuxGrouping
    // build mapping for DeclarativeAggregationFunction binding references
    val argsMapping = buildAggregateArgsMapping(
      isMerge, grouping.length, inputRelDataType, auxGrouping, aggregateCalls, aggBufferTypes)
    val aggBuffMapping = buildAggregateAggBuffMapping(aggBufferTypes)
    // gen code to create empty agg buffer
    val initedAggBuffer = genReusableEmptyAggBuffer(
      ctx, config, builder, inputTerm, inputType, auxGrouping, aggregates, aggBufferRowType)
    if (auxGrouping.isEmpty) {
      // create an empty agg buffer and initialized make it reusable
      ctx.addReusableOpenStatement(initedAggBuffer.code)
    }
    // gen code to update agg buffer from the aggregate map
    val aggregate = genAggregate(isMerge, ctx, config, builder, inputRelDataType,
      inputType, inputTerm, auxGrouping, aggregates, aggCallToAggFunction,
      argsMapping, aggBuffMapping, currentAggBufferTerm, aggBufferRowType)
    val outputExpr = genHashAggOutputExpr(isMerge, isFinal, ctx, config, builder, inputRelDataType,
      auxGrouping, aggregates, argsMapping, aggBuffMapping, outputTerm, outputType, inputTerm,
      inputType, Some(groupKeyTerm), aggBufferTerm, aggBufferRowType)
    (initedAggBuffer, aggregate, outputExpr)
  }

  // ===============================================================================================

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
      config: TableConfig,
      builder: RelBuilder,
      groupingAndAuxGrouping: (Array[Int], Array[Int]),
      inputRelDataType: RelDataType,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[AggregateFunction[_, _], String],
      logTerm: String,
      aggregateMapTerm: String,
      aggMapKVTypesTerm: (String, String),
      aggMapKVRowType: (RowType, RowType),
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
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
        ctx, config, builder, grouping, auxGrouping, inputRelDataType, aggCallToAggFunction,
        aggregates, udaggs, aggregateMapTerm, (groupKeyRowType, aggBufferRowType), aggregateMapTerm,
        sorterTerm, outputTerm, outputType, aggBufferNames, aggBufferTypes)

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
       |    new $binaryRowSerializerTypeTerm($groupKeyTypesTerm),
       |    new $binaryRowSerializerTypeTerm($aggBufferTypesTerm),
       |    $keyComputerTerm, $recordComparatorTerm,
       |    getContainingTask().getEnvironment().getMemoryManager().getPageSize(),
       |    getSqlConf()
       |  );
       """.stripMargin
  }

  private[flink] def genFallbackToSortAgg(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      inputRelDataType: RelDataType,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[AggregateFunction[_, _], String],
      mapTerm: String,
      mapKVRowTypes: (RowType, RowType),
      aggregateMapTerm: String,
      sorterTerm: String,
      outputTerm: String,
      outputType: RowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]]): String = {
    val (groupKeyRowType, aggBufferRowType) = mapKVRowTypes
    val keyTerm = CodeGenUtils.newName("key")
    val lastKeyTerm = CodeGenUtils.newName("lastKey")
    val keyNotEquals = genGroupKeyChangedCheckCode(keyTerm, lastKeyTerm)

    val joinedRow = classOf[JoinedRow].getName
    val fallbackInputTerm = ctx.newReusableField("fallbackInput", joinedRow)
    val fallbackInputType = new RowType(
      groupKeyRowType.getFieldTypes ++ aggBufferRowType.getFieldTypes,
      groupKeyRowType.getFieldNames ++ aggBufferRowType.getFieldNames)

    val (initAggBufferCode, updateAggBufferCode, resultExpr) = genSortAggCodes(
      isMerge = true, isFinal = true, ctx, config, builder, grouping, auxGrouping, inputRelDataType,
      aggCallToAggFunction, aggregates, udaggs, fallbackInputTerm, fallbackInputType,
      aggBufferNames, aggBufferTypes, outputType, forHashAgg = true)

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
       |      ${OperatorCodeGenerator.generatorCollect(outputTerm)}
       |      // found new group
       |      $lastKeyTerm = $keyTerm.copy();
       |      $initAggBufferCode
       |    }
       |    // reusable field access codes for agg buffer merge
       |    ${ctx.reuseInputUnboxingCode(Set(fallbackInputTerm))}
       |    // merge aggregate map's value into aggregate buffer fields
       |    $updateAggBufferCode
       |  }
       |
       |  // output last key group aggregate result
       |  ${resultExpr.code}
       |  $outputTerm.replace($lastKeyTerm, ${resultExpr.resultTerm});
       |  ${OperatorCodeGenerator.generatorCollect(outputTerm)}
       """.stripMargin
  }

  private[flink] def genKVSorterPrepareCode(
      ctx: CodeGeneratorContext,
      keyComputerTerm: String,
      recordComparatorTerm: String,
      aggMapKeyType: RowType) : String = {
    val keyFieldTypes = aggMapKeyType.getFieldInternalTypes
    val keys = keyFieldTypes.indices.toArray
    val orders = keys.map((_) => true)
    val nullsIsLast = SortUtil.getNullDefaultOrders(orders)
    val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
      keyFieldTypes.length, keys, orders, keyFieldTypes)

    val sortCodeGenerator = new SortCodeGenerator(
      keys, keyFieldTypes, comparators, orders, nullsIsLast)
    val genedSorter = GeneratedSorter(
      sortCodeGenerator.generateNormalizedKeyComputer("AggMapKeyComputer"),
      sortCodeGenerator.generateRecordComparator("AggMapValueComparator"),
      serializers,
      comparators)

    val keyComputerTypeTerm = classOf[NormalizedKeyComputer].getName
    val keyComputeInnerClassTerm = genedSorter.computer.name
    val recordComparatorTypeTerm = classOf[RecordComparator].getName
    val recordComparatorInnerClassTerm = genedSorter.comparator.name
    ctx.addReusableInnerClass(keyComputeInnerClassTerm, genedSorter.computer.code)
    ctx.addReusableInnerClass(recordComparatorInnerClassTerm, genedSorter.comparator.code)

    val serArrayTerm = s"${classOf[TypeSerializer[_]].getCanonicalName}[]"
    val compArrayTerm = s"${classOf[TypeComparator[_]].getCanonicalName}[]"
    val serializersTerm = ctx.addReferenceObj(serializers, serArrayTerm)
    val comparatorsTerm = ctx.addReferenceObj(comparators, compArrayTerm)

    s"""
       |  $keyComputerTypeTerm $keyComputerTerm = new $keyComputeInnerClassTerm();
       |  $recordComparatorTypeTerm $recordComparatorTerm = new $recordComparatorInnerClassTerm();
       |  $keyComputerTerm.init($serializersTerm, $comparatorsTerm);
       |  $recordComparatorTerm.init($serializersTerm, $comparatorsTerm);
       |""".stripMargin
  }
}
