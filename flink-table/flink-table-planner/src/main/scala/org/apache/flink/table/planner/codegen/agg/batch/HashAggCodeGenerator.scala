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

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.{JBoolean, JDouble, JLong}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, OperatorCodeGenerator, ProjectionCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.ROW_DATA
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.planner.typeutils.RowTypeUtils
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.TableStreamOperator
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMapSpillMemorySegmentPool
import org.apache.flink.table.runtime.util.collections.binary.BytesMap
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.tools.RelBuilder

/**
 * Operator code generator for HashAggregation, Only deal with [[DeclarativeAggregateFunction]] and
 * aggregateBuffers should be update(e.g.: setInt) in [[BinaryRowData]]. (Hash Aggregate performs
 * much better than Sort Aggregate).
 */
object HashAggCodeGenerator {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED: ConfigOption[JBoolean] =
    key("table.exec.local-hash-agg.adaptive.enabled")
      .booleanType()
      .defaultValue(Boolean.box(true))
      .withDescription(
        s"""
           |Whether to enable adaptive local hash aggregation. Adaptive local hash
           |aggregation is an optimization of local hash aggregation, which can adaptively 
           |determine whether to continue to do local hash aggregation according to the distinct
           | value rate of sampling data. If distinct value rate bigger than defined threshold
           |(see parameter: table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold), 
           |we will stop aggregating and just send the input data to the downstream after a simple 
           |projection. Otherwise, we will continue to do aggregation. Adaptive local hash aggregation
           |only works in batch mode. Default value of this parameter is true.
           |""".stripMargin)

  @Experimental
  val TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD: ConfigOption[JLong] =
    key("table.exec.local-hash-agg.adaptive.sampling-threshold")
      .longType()
      .defaultValue(Long.box(500000L))
      .withDescription(
        s"""
           |If adaptive local hash aggregation is enabled, this value defines how 
           |many records will be used as sampled data to calculate distinct value rate 
           |(see parameter: table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold) 
           |for the local aggregate. The higher the sampling threshold, the more accurate 
           |the distinct value rate is. But as the sampling threshold increases, local 
           |aggregation is meaningless when the distinct values rate is low. 
           |The default value is 500000.
           |""".stripMargin)

  @Experimental
  val TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD: ConfigOption[JDouble] =
    key("table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold")
      .doubleType()
      .defaultValue(0.5d)
      .withDescription(
        s"""
           |The distinct value rate can be defined as the number of local 
           |aggregation result for the sampled data divided by the sampling 
           |threshold (see ${TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD.key()}). 
           |If the computed result is lower than the given configuration value, 
           |the remaining input records proceed to do local aggregation, otherwise 
           |the remaining input records are subjected to simple projection which 
           |calculation cost is less than local aggregation. The default value is 0.5.
           |""".stripMargin)

  def genWithKeys(
      ctx: CodeGeneratorContext,
      builder: RelBuilder,
      aggInfoList: AggregateInfoList,
      inputType: RowType,
      outputType: RowType,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      isMerge: Boolean,
      isFinal: Boolean,
      supportAdaptiveLocalHashAgg: Boolean,
      maxNumFileHandles: Int,
      compressionEnabled: Boolean,
      compressionBlockSize: Int): GeneratedOperator[OneInputStreamOperator[RowData, RowData]] = {

    val aggInfos = aggInfoList.aggInfos
    val functionIdentifiers = AggCodeGenHelper.getFunctionIdentifiers(aggInfos)
    val aggBufferPrefix = "hash"
    val aggBufferNames = AggCodeGenHelper.getAggBufferNames(aggBufferPrefix, auxGrouping, aggInfos)
    val aggBufferTypes = AggCodeGenHelper.getAggBufferTypes(inputType, auxGrouping, aggInfos)
    val groupKeyRowType = RowTypeUtils.projectRowType(inputType, grouping)
    val aggBufferRowType = RowType.of(aggBufferTypes.flatten, aggBufferNames.flatten)

    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val className = if (isFinal) "HashAggregateWithKeys" else "LocalHashAggregateWithKeys"

    // add logger
    val logTerm = CodeGenUtils.newName("LOG")
    ctx.addReusableLogger(logTerm, className)

    // gen code to do group key projection from input
    val currentKeyTerm = CodeGenUtils.newName("currentKey")
    val currentKeyWriterTerm = CodeGenUtils.newName("currentKeyWriter")
    // currentValueTerm and currentValueWriterTerm are used for value
    // projection while supportAdaptiveLocalHashAgg is true.
    val currentValueTerm = CodeGenUtils.newName("currentValue")
    val currentValueWriterTerm = CodeGenUtils.newName("currentValueWriter")
    val keyProjectionCode = ProjectionCodeGenerator
      .generateProjectionExpression(
        ctx,
        inputType,
        groupKeyRowType,
        grouping,
        inputTerm = inputTerm,
        outRecordTerm = currentKeyTerm,
        outRecordWriterTerm = currentKeyWriterTerm)
      .code

    // gen code to create groupKey, aggBuffer Type array
    // it will be used in BytesHashMap and BufferedKVExternalSorter if enable fallback
    val groupKeyTypesTerm = CodeGenUtils.newName("groupKeyTypes")
    val aggBufferTypesTerm = CodeGenUtils.newName("aggBufferTypes")
    HashAggCodeGenHelper.prepareHashAggKVTypes(
      ctx,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      groupKeyRowType,
      aggBufferRowType)

    val binaryRowTypeTerm = classOf[BinaryRowData].getName
    // gen code to aggregate and output using hash map
    val aggregateMapTerm = CodeGenUtils.newName("aggregateMap")
    val lookupInfoTypeTerm = classOf[BytesMap.LookupInfo[_, _]].getCanonicalName
    val lookupInfo = ctx.addReusableLocalVariable(lookupInfoTypeTerm, "lookupInfo")
    HashAggCodeGenHelper.prepareHashAggMap(
      ctx,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      aggregateMapTerm)

    val outputTerm = CodeGenUtils.newName("hashAggOutput")
    val (reuseGroupKeyTerm, reuseAggBufferTerm) =
      HashAggCodeGenHelper.prepareTermForAggMapIteration(
        ctx,
        outputTerm,
        outputType,
        if (grouping.isEmpty) classOf[GenericRowData] else classOf[JoinedRowData])

    val currentAggBufferTerm = ctx.addReusableLocalVariable(binaryRowTypeTerm, "currentAggBuffer")
    val (initedAggBuffer, aggregate, outputExpr) = HashAggCodeGenHelper.genHashAggCodes(
      isMerge,
      isFinal,
      ctx,
      builder,
      (grouping, auxGrouping),
      inputTerm,
      inputType,
      aggInfos,
      currentAggBufferTerm,
      aggBufferRowType,
      aggBufferTypes,
      outputTerm,
      outputType,
      reuseGroupKeyTerm,
      reuseAggBufferTerm
    )

    val outputResultFromMap = HashAggCodeGenHelper.genAggMapIterationAndOutput(
      ctx,
      isFinal,
      aggregateMapTerm,
      reuseGroupKeyTerm,
      reuseAggBufferTerm,
      outputExpr)

    // gen code to deal with hash map oom, if enable fallback we will use sort agg strategy
    val sorterTerm = CodeGenUtils.newName("sorter")
    val retryAppend = HashAggCodeGenHelper.genRetryAppendToMap(
      aggregateMapTerm,
      currentKeyTerm,
      initedAggBuffer,
      lookupInfo,
      currentAggBufferTerm)

    val (dealWithAggHashMapOOM, fallbackToSortAggCode) = HashAggCodeGenHelper.genAggMapOOMHandling(
      isFinal,
      ctx,
      builder,
      (grouping, auxGrouping),
      aggInfos,
      functionIdentifiers,
      logTerm,
      aggregateMapTerm,
      (groupKeyTypesTerm, aggBufferTypesTerm),
      (groupKeyRowType, aggBufferRowType),
      aggBufferPrefix,
      aggBufferNames,
      aggBufferTypes,
      outputTerm,
      outputType,
      outputResultFromMap,
      sorterTerm,
      retryAppend,
      maxNumFileHandles,
      compressionEnabled,
      compressionBlockSize
    )

    HashAggCodeGenHelper.prepareMetrics(ctx, aggregateMapTerm, if (isFinal) sorterTerm else null)

    // Do adaptive hash aggregation
    val outputResultForAdaptiveLocalHashAgg = {
      // gen code to iterating the aggregate map and output to downstream
      val inputUnboxingCode = s"${ctx.reuseInputUnboxingCode(reuseAggBufferTerm)}"
      s"""
         |   // set result and output
         |   $reuseGroupKeyTerm =  ($ROW_DATA)$currentKeyTerm;
         |   $reuseAggBufferTerm = ($ROW_DATA)$currentValueTerm;
         |   $inputUnboxingCode
         |   ${outputExpr.code}
         |   ${OperatorCodeGenerator.generateCollect(outputExpr.resultTerm)}
         |
       """.stripMargin
    }
    val localAggSuppressedTerm = CodeGenUtils.newName("localAggSuppressed")
    ctx.addReusableMember(s"private transient boolean $localAggSuppressedTerm = false;")
    val valueProjectionCode =
      if (!isFinal && supportAdaptiveLocalHashAgg) {
        ProjectionCodeGenerator.genAdaptiveLocalHashAggValueProjectionCode(
          ctx,
          inputType,
          classOf[BinaryRowData],
          inputTerm = inputTerm,
          aggInfos,
          auxGrouping,
          outRecordTerm = currentValueTerm,
          outRecordWriterTerm = currentValueWriterTerm
        )
      } else {
        ""
      }

    val (
      distinctCountIncCode,
      totalCountIncCode,
      adaptiveSamplingCode,
      adaptiveLocalHashAggCode,
      flushResultSuppressEnableCode) = {
      // from these conditions we know that it must be a distinct operation
      if (
        !isFinal &&
        ctx.tableConfig.get(TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED) &&
        supportAdaptiveLocalHashAgg
      ) {
        val adaptiveDistinctCountTerm = CodeGenUtils.newName("distinctCount")
        val adaptiveTotalCountTerm = CodeGenUtils.newName("totalCount")
        ctx.addReusableMember(s"private transient long $adaptiveDistinctCountTerm = 0;")
        ctx.addReusableMember(s"private transient long $adaptiveTotalCountTerm = 0;")

        val samplingThreshold =
          ctx.tableConfig.get(TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD)
        val distinctValueRateThreshold =
          ctx.tableConfig.get(TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_DISTINCT_VALUE_RATE_THRESHOLD)

        (
          s"$adaptiveDistinctCountTerm++;",
          s"$adaptiveTotalCountTerm++;",
          s"""
             |if ($adaptiveTotalCountTerm == $samplingThreshold) {
             |  $logTerm.info("Local hash aggregation checkpoint reached, sampling threshold = " +
             |    $samplingThreshold + ", distinct value count = " + $adaptiveDistinctCountTerm + ", total = " +
             |    $adaptiveTotalCountTerm + ", distinct value rate threshold = " 
             |    + $distinctValueRateThreshold);
             |  if ($adaptiveDistinctCountTerm / (1.0 * $adaptiveTotalCountTerm) > $distinctValueRateThreshold) {
             |    $logTerm.info("Local hash aggregation is suppressed");
             |    $localAggSuppressedTerm = true;
             |  }
             |}
             |""".stripMargin,
          s"""
             |if ($localAggSuppressedTerm) {
             |  $valueProjectionCode
             |  $outputResultForAdaptiveLocalHashAgg
             |  return;
             |}
             |""".stripMargin,
          s"""
             |if ($localAggSuppressedTerm) {
             |  $outputResultFromMap
             |  return;
             |}
             |""".stripMargin)
      } else {
        ("", "", "", "", "")
      }
    }

    val lazyInitAggBufferCode = if (auxGrouping.nonEmpty) {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBuffer.code}
       """.stripMargin
    } else {
      ""
    }

    val processCode =
      s"""
         | // input field access for group key projection and aggregate buffer update
         |${ctx.reuseInputUnboxingCode(inputTerm)}
         | // project key from input
         |$keyProjectionCode
         |
         |$adaptiveLocalHashAggCode
         |
         | // look up output buffer using current group key
         |$lookupInfo = ($lookupInfoTypeTerm) $aggregateMapTerm.lookup($currentKeyTerm);
         |$currentAggBufferTerm = ($binaryRowTypeTerm) $lookupInfo.getValue();
         |
         |if (!$lookupInfo.isFound()) {
         |  $distinctCountIncCode
         |  $lazyInitAggBufferCode
         |  // append empty agg buffer into aggregate map for current group key
         |  try {
         |    $currentAggBufferTerm =
         |      $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
         |  } catch (java.io.EOFException exp) {
         |    $dealWithAggHashMapOOM
         |  }
         |}
         |
         |$totalCountIncCode
         |$adaptiveSamplingCode
         |
         | // aggregate buffer fields access
         |${ctx.reuseInputUnboxingCode(currentAggBufferTerm)}
         | // do aggregate and update agg buffer
         |${aggregate.code}
         | // flush result form map if suppress is enable. 
         |$flushResultSuppressEnableCode
         |""".stripMargin.trim

    val endInputCode = if (isFinal) {
      val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
      s"""
         |if ($sorterTerm == null) {
         | // no spilling, output by iterating aggregate map.
         | $outputResultFromMap
         |} else {
         |  // spill last part of input' aggregation output buffer
         |  $sorterTerm.sortAndSpill(
         |    $aggregateMapTerm.getRecordAreaMemorySegments(),
         |    $aggregateMapTerm.getNumElements(),
         |    new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments()));
         |   // only release floating memory in advance.
         |   $aggregateMapTerm.free(true);
         |  // fall back to sort based aggregation
         |  $fallbackToSortAggCode
         |}
       """.stripMargin
    } else {
      s"""
         |if (!$localAggSuppressedTerm) {
         | $outputResultFromMap
         |}
         |""".stripMargin
    }

    AggCodeGenHelper.generateOperator(
      ctx,
      className,
      classOf[TableStreamOperator[RowData]].getCanonicalName,
      processCode,
      endInputCode,
      inputType)
  }
}
