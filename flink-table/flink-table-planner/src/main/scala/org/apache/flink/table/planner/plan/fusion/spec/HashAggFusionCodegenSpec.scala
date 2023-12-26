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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{getReuseRowFieldExprs, newName, newNames}
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator.genAdaptiveLocalHashAggValueProjectionExpr
import org.apache.flink.table.planner.codegen.agg.batch.{AggCodeGenHelper, HashAggCodeGenHelper}
import org.apache.flink.table.planner.codegen.agg.batch.AggCodeGenHelper.{buildAggregateArgsMapping, genAggregateByFlatAggregateBuffer, genFlatAggBufferExprs, genGetValueFromFlatAggregateBuffer, genInitFlatAggregateBuffer}
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator.{TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD, TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED, TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD}
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenHelper.{buildAggregateAggBuffMapping, genAggregate, genCreateFallbackSorter, genHashAggValueExpr, genRetryAppendToMap, genReusableEmptyAggBuffer, prepareFallbackSorter}
import org.apache.flink.table.planner.plan.fusion.{OpFusionCodegenSpecBase, OpFusionContext}
import org.apache.flink.table.planner.plan.fusion.FusionCodegenUtil.{constructDoConsumeCode, constructDoConsumeFunction, evaluateVariables}
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMapSpillMemorySegmentPool
import org.apache.flink.table.runtime.util.KeyValueIterator
import org.apache.flink.table.runtime.util.collections.binary.{BytesHashMap, BytesMap}
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.tools.RelBuilder

import java.util

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/** The operator fusion codegen spec for HashAgg. */
class HashAggFusionCodegenSpec(
    opCodegenCtx: CodeGeneratorContext,
    builder: RelBuilder,
    aggInfoList: AggregateInfoList,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isFinal: Boolean,
    isMerge: Boolean,
    supportAdaptiveLocalHashAgg: Boolean,
    maxNumFileHandles: Int,
    compressionEnabled: Boolean,
    compressionBlockSize: Int)
  extends OpFusionCodegenSpecBase(opCodegenCtx) {

  private lazy val aggBufferPrefix: String = if (isFinal) { newName("hash") }
  else { newName("local_hash") }

  private lazy val aggInfos = aggInfoList.aggInfos
  private lazy val functionIdentifiers = AggCodeGenHelper.getFunctionIdentifiers(aggInfos)
  private lazy val aggBufferNames =
    AggCodeGenHelper.getAggBufferNames(aggBufferPrefix, auxGrouping, aggInfos)

  // The name for BytesMap
  private lazy val aggregateMapTerm: String = newName("aggregateMap")

  private var inputContext: OpFusionContext = _
  private var inputType: RowType = _
  private var aggBufferTypes: Array[Array[LogicalType]] = _
  private var groupKeyRowType: RowType = _
  private var aggBufferRowType: RowType = _
  private var argsMapping: Array[Array[(Int, LogicalType)]] = _
  private var aggBuffMapping: Array[Array[(Int, LogicalType)]] = _

  private var inputRowTerm: String = _
  private var outputFromMap: String = _
  private var aggBufferExprs: Seq[GeneratedExpression] = _
  private var consumeFunctionName: String = _
  private var initAggBufferCode: String = _
  private var hasInput: String = _
  private var sorterTerm: String = _
  private var localAggSuppressedTerm: String = _

  override def variablePrefix: String = if (isFinal) { "hashagg" }
  else { "local_hashagg" }

  override def setup(opFusionContext: OpFusionContext): Unit = {
    super.setup(opFusionContext)
    assert(opFusionContext.getInputFusionContexts.size == 1)
    inputContext = opFusionContext.getInputFusionContexts.head

    inputType = inputContext.getOutputType
    aggBufferTypes = AggCodeGenHelper.getAggBufferTypes(inputType, auxGrouping, aggInfos)
    groupKeyRowType = RowTypeUtils.projectRowType(inputType, grouping)
    aggBufferRowType = RowType.of(aggBufferTypes.flatten, aggBufferNames.flatten)
    argsMapping = buildAggregateArgsMapping(
      isMerge,
      grouping.length,
      inputType,
      auxGrouping,
      aggInfos,
      aggBufferTypes)
    aggBuffMapping = buildAggregateAggBuffMapping(aggBufferTypes)
  }

  override def doProcessProduce(fusionCtx: CodeGeneratorContext): Unit = {
    inputContext.processProduce(fusionCtx)
  }

  override def doEndInputProduce(fusionCtx: CodeGeneratorContext): Unit = {
    inputContext.endInputProduce(fusionCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    inputRowTerm = row.resultTerm
    if (grouping.isEmpty) {
      doProcessConsumeWithoutKeys()
    } else {
      doProcessConsumeWithKeys(toScala(inputVars))
    }
  }

  override def doEndInputConsume(inputId: Int): String = {
    if (grouping.isEmpty) {
      doEndInputConsumeWithoutKeys()
    } else {
      doEndInputConsumeWithKeys()
    }
  }

  /** Generate the java source code to process the row with group key. */
  private def doProcessConsumeWithKeys(input: Seq[GeneratedExpression]): String = {
    // initialize the hashmap related code first
    val Seq(groupKeyTypesTerm, aggBufferTypesTerm) = newNames("groupKeyTypes", "aggBufferTypes")
    // gen code to create groupKey, aggBuffer Type array, it will be used in BytesHashMap and BufferedKVExternalSorter if enable fallback
    HashAggCodeGenHelper.prepareHashAggKVTypes(
      opCodegenCtx,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      groupKeyRowType,
      aggBufferRowType)

    // create aggregate map
    val memorySizeTerm = newName("memorySize")
    val mapTypeTerm = classOf[BytesHashMap].getName
    opCodegenCtx.addReusableMember(s"private transient $mapTypeTerm $aggregateMapTerm;")
    opCodegenCtx.addReusableOpenStatement(
      s"""
         |long $memorySizeTerm = computeMemorySize(${fusionContext.getManagedMemoryFraction});
         |$aggregateMapTerm = new $mapTypeTerm(
         |  getContainingTask(),
         |  getContainingTask().getEnvironment().getMemoryManager(),
         |  $memorySizeTerm,
         |  $groupKeyTypesTerm,
         |  $aggBufferTypesTerm);
       """.stripMargin)

    // close aggregate map and release memory segments
    opCodegenCtx.addReusableCloseStatement(s"$aggregateMapTerm.free();")

    val Seq(currentKeyTerm, currentKeyWriterTerm) = newNames("currentKey", "currentKeyWriter")
    val Seq(lookupInfo, currentAggBufferTerm) = newNames("lookupInfo", "currentAggBuffer")
    val lookupInfoTypeTerm = classOf[BytesMap.LookupInfo[_, _]].getCanonicalName
    val binaryRowTypeTerm = classOf[BinaryRowData].getName

    // evaluate input field access for group key projection and aggregate buffer update
    val inputAccessCode = evaluateVariables(input)
    // project key row from input
    val keyExprs = grouping.map(idx => input(idx))
    val keyProjectionCode = getExprCodeGenerator
      .generateResultExpression(
        keyExprs,
        groupKeyRowType,
        classOf[BinaryRowData],
        currentKeyTerm,
        outRowWriter = Option(currentKeyWriterTerm))
      .code

    // gen code to create empty agg buffer, here need to consider the auxGrouping is not empty case
    val initedAggBuffer = genReusableEmptyAggBuffer(
      opCodegenCtx,
      builder,
      inputRowTerm,
      inputType,
      auxGrouping,
      aggInfos,
      aggBufferRowType)
    val lazyInitAggBufferCode = if (auxGrouping.isEmpty) {
      // create an empty agg buffer and initialized make it reusable
      opCodegenCtx.addReusableOpenStatement(initedAggBuffer.code)
      ""
    } else {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBuffer.code}
       """.stripMargin
    }

    // generate code to update agg buffer
    opCodegenCtx.startNewLocalVariableStatement(currentAggBufferTerm)
    val aggregateExpr = genAggregate(
      isMerge,
      opCodegenCtx,
      builder,
      inputType,
      inputRowTerm,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBuffMapping,
      currentAggBufferTerm,
      aggBufferRowType
    )

    // gen code to prepare agg output using agg buffer and key from the aggregate map
    val Seq(reuseAggMapKeyTerm, reuseAggBufferTerm) = newNames("reuseAggMapKey", "reuseAggBuffer")
    val rowData = classOf[RowData].getName
    opCodegenCtx.addReusableMember(s"private transient $rowData $reuseAggMapKeyTerm;")
    opCodegenCtx.addReusableMember(s"private transient $rowData $reuseAggBufferTerm;")
    // gen code to prepare agg output using agg buffer and key from the aggregate map
    val iteratorTerm = CodeGenUtils.newName("iterator")
    val iteratorType = classOf[KeyValueIterator[_, _]].getCanonicalName

    opCodegenCtx.startNewLocalVariableStatement(reuseAggBufferTerm)
    val reuseKeyExprs = getReuseRowFieldExprs(opCodegenCtx, groupKeyRowType, reuseAggMapKeyTerm)
    // get value expr from agg buffer
    getExprCodeGenerator
      .bindSecondInput(aggBufferRowType, reuseAggBufferTerm)
    val reuseValueExprs = genHashAggValueExpr(
      isMerge,
      isFinal,
      opCodegenCtx,
      getExprCodeGenerator,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBuffMapping,
      inputType,
      reuseAggBufferTerm,
      aggBufferRowType
    )
    // reuse aggBuffer field access code if isFinal to avoid evaluate more times
    val reuseAggBufferFieldCode = if (isFinal) {
      opCodegenCtx.reuseInputUnboxingCode(reuseAggBufferTerm)
    } else {
      ""
    }
    outputFromMap =
      s"""
         |${opCodegenCtx.reuseLocalVariableCode(reuseAggBufferTerm)}
         |$iteratorType<$rowData, $rowData> $iteratorTerm =
         |  $aggregateMapTerm.getEntryIterator(false); // reuse key/value during iterating
         |while ($iteratorTerm.advanceNext()) {
         |   // set result and output
         |   $reuseAggMapKeyTerm = ($rowData)$iteratorTerm.getKey();
         |   $reuseAggBufferTerm = ($rowData)$iteratorTerm.getValue();
         |   // consume the row of agg produce
         |   $reuseAggBufferFieldCode
         |   ${outputResult(fusionContext.getOutputType, reuseKeyExprs ++ reuseValueExprs)}
         |}
       """.stripMargin

    val retryAppendCode = genRetryAppendToMap(
      aggregateMapTerm,
      currentKeyTerm,
      initedAggBuffer,
      lookupInfo,
      currentAggBufferTerm)

    // gen code to deal with hash map oom, if enable fallback we will use sort agg strategy
    val dealWithAggHashMapOOM =
      genHashAggOOMHandling(groupKeyTypesTerm, aggBufferTypesTerm, retryAppendCode)

    // generate the adaptive local hash agg code
    localAggSuppressedTerm = newName("localAggSuppressed")
    opCodegenCtx.addReusableMember(s"private transient boolean $localAggSuppressedTerm = false;")
    val (
      distinctCountIncCode,
      totalCountIncCode,
      adaptiveSamplingCode,
      adaptiveLocalHashAggCode,
      flushResultSuppressEnableCode) = genAdaptiveLocalHashAgg(keyExprs)

    // process code
    s"""
       |do {
       |   // input field access
       |  $inputAccessCode
       |
       |  $adaptiveLocalHashAggCode
       |
       |  // project key from input
       |  $keyProjectionCode
       |
       |   // lookup output buffer using current group key
       |  $lookupInfoTypeTerm $lookupInfo = ($lookupInfoTypeTerm) $aggregateMapTerm.lookup($currentKeyTerm);
       |  $binaryRowTypeTerm $currentAggBufferTerm = ($binaryRowTypeTerm) $lookupInfo.getValue();
       |  if (!$lookupInfo.isFound()) {
       |    $distinctCountIncCode
       |    $lazyInitAggBufferCode
       |    // append empty agg buffer into aggregate map for current group key
       |    try {
       |      $currentAggBufferTerm =
       |        $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
       |    } catch (java.io.EOFException exp) {
       |      $dealWithAggHashMapOOM
       |    }
       |  }
       |
       |  $totalCountIncCode
       |  $adaptiveSamplingCode
       |
       |  // do aggregate and update agg buffer
       |  ${opCodegenCtx.reuseLocalVariableCode(currentAggBufferTerm)}
       |  // aggregate buffer fields access
       |  ${opCodegenCtx.reuseInputUnboxingCode(currentAggBufferTerm)}
       |  
       |  ${aggregateExpr.code}
       |  // flush result form map if suppress is enable.
       |  $flushResultSuppressEnableCode
       |} while(false);
       |""".stripMargin.trim
  }

  /** Generate the java source code to trigger the clean work with group key. */
  private def doEndInputConsumeWithKeys(): String = {
    val endInputCode = if (isFinal) {
      val fallbackToSortAggCode = genFallbackToSortAgg()
      val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
      s"""
         |if ($sorterTerm == null) {
         | // no spilling, output by iterating aggregate map.
         |  $outputFromMap
         |} else {
         |  // spill last part of input' aggregation output buffer
         |  $sorterTerm.sortAndSpill(
         |    $aggregateMapTerm.getRecordAreaMemorySegments(),
         |    $aggregateMapTerm.getNumElements(),
         |    new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments()));
         |    // only release floating memory in advance.
         |  $aggregateMapTerm.free(true);
         |   
         |  // fall back to sort based aggregation
         |  $fallbackToSortAggCode
         |}
       """.stripMargin
    } else {
      s"""
         |  if (!$localAggSuppressedTerm) {
         |    $outputFromMap
         |  }
         |""".stripMargin
    }

    val endInputMethodTerm = newName(variablePrefix + "withKeyEndInput")
    opCodegenCtx.addReusableMember(
      s"""
         |private void $endInputMethodTerm() throws Exception {
         |  $endInputCode
         |}
       """.stripMargin
    )
    // we also need to call downstream endInput method
    s"""
       |$endInputMethodTerm();
       |  // call downstream endInput
       |${fusionContext.endInputConsume()}
       """.stripMargin
  }

  /** Generate the java source code to process the row without group key. */
  private def doProcessConsumeWithoutKeys(): String = {
    val aggregateCode = genFallbackSortAggCode(isMerge, forHashAgg = false)
    hasInput = newName("hasInput")
    opCodegenCtx.addReusableMember(s"private boolean $hasInput = false;")
    s"""
       |${opCodegenCtx.reuseLocalVariableCode()}
       |if (!$hasInput) {
       |  $hasInput = true;
       |  // init agg buffer
       |  $initAggBufferCode
       |}
       |// update agg buffer to do aggregate
       |$aggregateCode
       |""".stripMargin.trim
  }

  /** Generate the java source code to trigger the clean work without group key. */
  private def doEndInputConsumeWithoutKeys(): String = {
    val (localVariables, resultExprs) = if (isFinal) {
      val endInputOutputRowTerm = newName("endInputOutputRowTerm")
      opCodegenCtx.startNewLocalVariableStatement(endInputOutputRowTerm)
      val exprs = genGetValueFromFlatAggregateBuffer(
        isMerge,
        opCodegenCtx,
        builder,
        auxGrouping,
        aggInfos,
        functionIdentifiers,
        argsMapping,
        aggBufferPrefix,
        aggBufferNames,
        aggBufferTypes,
        fusionContext.getOutputType
      )
      (opCodegenCtx.reuseLocalVariableCode(endInputOutputRowTerm), exprs)
    } else {
      ("", aggBufferExprs)
    }

    val endInputCode = if (isFinal) {
      s"""
         |// if the input is empty in final phase, we should output default values
         |if (!$hasInput) {
         |  $initAggBufferCode
         |}
         |// consume the agg output 
         |${fusionContext.processConsume(toJava(resultExprs))}
       """.stripMargin
    } else {
      s"""
         |if ($hasInput) {
         |  ${fusionContext.processConsume(toJava(resultExprs))}
         |}
       """.stripMargin
    }

    val endInputMethodTerm = newName(variablePrefix + "EndInputWithoutKeys")
    opCodegenCtx.addReusableMember(
      s"""
         |private void $endInputMethodTerm() throws Exception {
         |  $localVariables
         |  $endInputCode
         |}
       """.stripMargin
    )
    // we also need to call downstream endInput method
    s"""
       |$endInputMethodTerm();
       |  // propagate to downstream endInput
       |${fusionContext.endInputConsume()}
       """.stripMargin
  }

  private def outputResult(resultType: RowType, resultVars: Seq[GeneratedExpression]): String = {
    if (consumeFunctionName == null) {
      consumeFunctionName =
        constructDoConsumeFunction(variablePrefix, opCodegenCtx, fusionContext, resultType)
    }
    constructDoConsumeCode(consumeFunctionName, resultVars)
  }

  private def genAdaptiveLocalHashAgg(
      keyExprs: Seq[GeneratedExpression]): (String, String, String, String, String) = {
    if (
      !isFinal && supportAdaptiveLocalHashAgg &&
      opCodegenCtx.tableConfig.get(TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED)
    ) {
      val adaptiveLocalAggRowTerm = newName("adaptiveLocalAggRowTerm")
      opCodegenCtx.startNewLocalVariableStatement(adaptiveLocalAggRowTerm)
      val projectionExprs =
        genAdaptiveLocalHashAggValueProjectionExpr(
          opCodegenCtx,
          inputType,
          inputRowTerm,
          aggInfos,
          auxGrouping)
      val adaptiveDistinctCountTerm = CodeGenUtils.newName("distinctCount")
      val adaptiveTotalCountTerm = CodeGenUtils.newName("totalCount")
      opCodegenCtx.addReusableMember(s"private transient long $adaptiveDistinctCountTerm = 0;")
      opCodegenCtx.addReusableMember(s"private transient long $adaptiveTotalCountTerm = 0;")

      val samplingThreshold =
        opCodegenCtx.tableConfig.get(TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD)
      val distinctValueRateThreshold =
        opCodegenCtx.tableConfig.get(
          TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD)
      (
        s"$adaptiveDistinctCountTerm++;",
        s"$adaptiveTotalCountTerm++;",
        s"""
           |if ($adaptiveTotalCountTerm == $samplingThreshold) {
           |  LOG.info("Local hash aggregation checkpoint reached, sampling threshold = " +
           |    $samplingThreshold + ", distinct value count = " + $adaptiveDistinctCountTerm + ", total = " +
           |    $adaptiveTotalCountTerm + ", distinct value rate threshold = "
           |    + $distinctValueRateThreshold);
           |  if ($adaptiveDistinctCountTerm / (1.0 * $adaptiveTotalCountTerm) > $distinctValueRateThreshold) {
           |    LOG.info("Local hash aggregation is suppressed");
           |    $localAggSuppressedTerm = true;
           |  }
           |}
           |""".stripMargin,
        s"""
           |if ($localAggSuppressedTerm) {
           |  ${opCodegenCtx.reuseLocalVariableCode(adaptiveLocalAggRowTerm)}
           |  ${outputResult(fusionContext.getOutputType, keyExprs ++ projectionExprs)}
           |  continue;
           |}
           |""".stripMargin,
        s"""
           |if ($localAggSuppressedTerm) {
           |  $outputFromMap
           |}
           |""".stripMargin)

    } else {
      ("", "", "", "", "")
    }
  }

  private def genHashAggOOMHandling(
      groupKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      retryAppendCode: String): String = {
    if (isFinal) {
      val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
      sorterTerm = CodeGenUtils.newName("sorter")
      prepareFallbackSorter(opCodegenCtx, sorterTerm)
      val createSorter = genCreateFallbackSorter(
        opCodegenCtx,
        groupKeyRowType,
        groupKeyTypesTerm,
        aggBufferTypesTerm,
        sorterTerm,
        maxNumFileHandles,
        compressionEnabled,
        compressionBlockSize)
      s"""
         |LOG.info("BytesHashMap out of memory with {} entries, start spilling.", $aggregateMapTerm.getNumElements());
         | // hash map out of memory, spill to external sorter
         |if ($sorterTerm == null) {
         |  $createSorter
         |}
         | // sort and spill
         |$sorterTerm.sortAndSpill(
         |  $aggregateMapTerm.getRecordAreaMemorySegments(),
         |  $aggregateMapTerm.getNumElements(),
         |  new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments()));
         |$retryAppendCode
       """.stripMargin
    } else {
      s"""
         |LOG.info("BytesHashMap out of memory with {} entries, output directly.", $aggregateMapTerm.getNumElements());
         | // hash map out of memory, output directly
         |$outputFromMap
         |$retryAppendCode
          """.stripMargin
    }
  }

  private def genFallbackToSortAgg(): String = {
    val Seq(keyTerm, lastKeyTerm) = newNames("key", "lastKey")
    val keyNotEquals = AggCodeGenHelper.genGroupKeyChangedCheckCode(keyTerm, lastKeyTerm)
    val joinedRow = classOf[JoinedRowData].getName
    inputRowTerm = newName("fallbackInput")
    inputType = RowType.of(
      (groupKeyRowType.getChildren ++ aggBufferRowType.getChildren).toArray,
      (groupKeyRowType.getFieldNames ++ aggBufferRowType.getFieldNames).toArray)
    opCodegenCtx.startNewLocalVariableStatement(inputRowTerm)

    argsMapping = buildAggregateArgsMapping(
      true,
      grouping.length,
      inputType,
      auxGrouping,
      aggInfos,
      aggBufferTypes)
    val updateAggBufferCode = genFallbackSortAggCode(true, true)

    val keyExprs = getReuseRowFieldExprs(opCodegenCtx, groupKeyRowType, lastKeyTerm)
    val valueExprs = genGetValueFromFlatAggregateBuffer(
      isMerge,
      opCodegenCtx,
      builder,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      argsMapping,
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes,
      fusionContext.getOutputType
    )
    val outputCode = outputResult(fusionContext.getOutputType, keyExprs ++ valueExprs)
    val kvPairTerm = CodeGenUtils.newName("kvPair")
    val kvPairTypeTerm = classOf[JTuple2[BinaryRowData, BinaryRowData]].getName
    val aggBuffTerm = CodeGenUtils.newName("val")
    val binaryRow = classOf[BinaryRowData].getName

    s"""
       |  $binaryRow $lastKeyTerm = null;
       |  $kvPairTypeTerm<$binaryRow, $binaryRow> $kvPairTerm = null;
       |  $binaryRow $keyTerm = null;
       |  $binaryRow $aggBuffTerm = null;
       |  $joinedRow $inputRowTerm = new $joinedRow();
       |  ${opCodegenCtx.reuseLocalVariableCode(inputRowTerm)}
       |  // free hash map memory, but not release back to memory manager
       |  org.apache.flink.util.MutableObjectIterator<$kvPairTypeTerm<$binaryRow, $binaryRow>>
       |    iterator = $sorterTerm.getKVIterator();
       |  while (
       |    ($kvPairTerm = ($kvPairTypeTerm<$binaryRow, $binaryRow>) iterator.next()) != null) {
       |    $keyTerm = ($binaryRow) $kvPairTerm.f0;
       |    $aggBuffTerm = ($binaryRow) $kvPairTerm.f1;
       |    // prepare input
       |    $inputRowTerm.replace($keyTerm, $aggBuffTerm);
       |    if ($lastKeyTerm == null) {
       |      // found first key group
       |      $lastKeyTerm = $keyTerm.copy();
       |      $initAggBufferCode
       |    } else if ($keyNotEquals) {
       |      // output current group aggregate result
       |      $outputCode
       |      // found new group
       |      $lastKeyTerm = $keyTerm.copy();
       |      $initAggBufferCode
       |    }
       |    // reusable field access codes for agg buffer merge
       |    ${opCodegenCtx.reuseInputUnboxingCode(inputRowTerm)}
       |    // merge aggregate map's value into aggregate buffer fields
       |    $updateAggBufferCode
       |  }
       |
       |  // output last key group aggregate result
       |  $outputCode
       """.stripMargin
  }

  private def genFallbackSortAggCode(isMerge: Boolean, forHashAgg: Boolean): String = {
    // The agg buffer field is member variable when without key
    aggBufferExprs = genFlatAggBufferExprs(
      isMerge,
      opCodegenCtx,
      builder,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes
    )

    initAggBufferCode = genInitFlatAggregateBuffer(
      opCodegenCtx,
      builder,
      inputType,
      inputRowTerm,
      grouping,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      aggBufferExprs,
      forHashAgg = forHashAgg
    )

    genAggregateByFlatAggregateBuffer(
      isMerge,
      opCodegenCtx,
      builder,
      inputType,
      inputRowTerm,
      auxGrouping,
      aggInfos,
      functionIdentifiers,
      argsMapping,
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs
    )
  }
}
