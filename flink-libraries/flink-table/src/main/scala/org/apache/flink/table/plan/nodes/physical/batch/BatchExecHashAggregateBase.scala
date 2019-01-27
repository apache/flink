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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{DataType, RowType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.BatchExecHashAggregateCodeGen
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.runtime.util.{BytesHashMap, BytesHashMapSpillMemorySegmentPool}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

abstract class BatchExecHashAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecGroupAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    isMerge,
    isFinal)
  with BatchExecHashAggregateCodeGen {

  lazy val aggBufferRowType: RowType = new RowType(
    aggBufferTypes.flatten.toArray[DataType], aggBufferNames.flatten)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    val hashCpuCost = HASH_CPU_COST * inputRowCnt * numOfGroupKey
    val aggFunctionCpuCost = FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = if (numOfGroupKey == 0) {
      averageRowSize
    } else {
      // assume memory is enough to hold hashTable to simplify the estimation because spill will not
      // happen under the assumption
      val ndvOfGroupKey = Util.first(mq.getRowCount(this), 1d)
      //  We aim for a 200% utilization of the bucket table.
      val bucketSize =
        ndvOfGroupKey * BytesHashMap.BUCKET_SIZE / BatchPhysicalRel.HASH_COLLISION_WEIGHT
      val recordSize = ndvOfGroupKey *
        (BatchPhysicalRel.binaryRowAverageSize(this) +  BytesHashMap.RECORD_EXTRA_LENGTH)
      bucketSize + recordSize
    }
    val cpuCost = hashCpuCost + aggFunctionCpuCost
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  def getOutputRowClass: Class[_ <: BaseRow] =
    if (grouping.isEmpty) classOf[GenericRow] else classOf[JoinedRow]

  def codegenWithKeys(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: RowType,
      outputType: RowType,
      reservedManagedMemory: Long,
      maxManagedMemory: Long): GeneratedOperator = {
    val config = tableEnv.config
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val className = if (isFinal) "HashAggregateWithKeys" else "LocalHashAggregateWithKeys"

    // add logger
    val logTerm = CodeGenUtils.newName("LOG")
    ctx.addReusableLogger(logTerm, className)

    // gen code to do group key projection from input
    val currentKeyTerm = CodeGenUtils.newName("currentKey")
    val currentKeyWriterTerm = CodeGenUtils.newName("currentKeyWriter")
    val keyProjectionCode = genGroupKeyProjectionCode("HashAgg", ctx, groupKeyRowType,
      getGrouping, inputType, inputTerm, currentKeyTerm, currentKeyWriterTerm)

    // ---------------------------------------------------------------------------------------------
    // gen code to create groupKey, aggBuffer Type array
    // it will be used in BytesHashMap and BufferedKVExternalSorter if enable fallback
    val groupKeyTypesTerm = CodeGenUtils.newName("groupKeyTypes")
    val aggBufferTypesTerm = CodeGenUtils.newName("aggBufferTypes")
    prepareHashAggKVTypes(
      ctx, groupKeyTypesTerm, aggBufferTypesTerm, groupKeyRowType, aggBufferRowType)

    // gen code to aggregate and output using hash map
    val aggregateMapTerm = CodeGenUtils.newName("aggregateMap")
    val lookupInfo = ctx.newReusableField(
      "lookupInfo",
      classOf[BytesHashMap.LookupInfo].getCanonicalName)
    prepareHashAggMap(
      ctx,
      config,
      reservedManagedMemory,
      maxManagedMemory,
      groupKeyTypesTerm,
      aggBufferTypesTerm,
      aggregateMapTerm)

    val outputTerm = CodeGenUtils.newName("hashAggOutput")
    val (reuseAggMapEntryTerm, reuseGroupKeyTerm, reuseAggBufferTerm) =
      prepareTermForAggMapIteration(ctx, outputTerm, outputType, groupKeyRowType, aggBufferRowType)

    val currentAggBufferTerm = ctx.newReusableField("currentAggBuffer", classOf[BinaryRow].getName)
    val (initedAggBuffer, aggregate, outputExpr) = genHashAggCodes(isMerge, isFinal, ctx, config,
      builder, (getGrouping, getAuxGrouping), inputRelDataType, inputTerm, inputType,
      aggregateCalls, aggCallToAggFunction, aggregates, currentAggBufferTerm, aggBufferRowType,
      aggBufferNames, aggBufferTypes, outputTerm, outputType, reuseGroupKeyTerm, reuseAggBufferTerm)

    val outputResultFromMap = genAggMapIterationAndOutput(
      ctx, config, isFinal, aggregateMapTerm, reuseAggMapEntryTerm, reuseAggBufferTerm, outputExpr)

    // ---------------------------------------------------------------------------------------------
    // gen code to deal with hash map oom, if enable fallback we will use sort agg strategy
    val sorterTerm = CodeGenUtils.newName("sorter")
    val retryAppend = genRetryAppendToMap(
      aggregateMapTerm, currentKeyTerm, initedAggBuffer, lookupInfo, currentAggBufferTerm)

    val (dealWithAggHashMapOOM, fallbackToSortAggCode) = genAggMapOOMHandling(
      isFinal, ctx, config, builder, (getGrouping, getAuxGrouping), inputRelDataType,
      aggCallToAggFunction, aggregates, udaggs, logTerm, aggregateMapTerm,
      (groupKeyTypesTerm, aggBufferTypesTerm), (groupKeyRowType, aggBufferRowType),
      aggBufferNames, aggBufferTypes, outputTerm, outputType, outputResultFromMap,
      sorterTerm, retryAppend)

    prepareMetrics(ctx, aggregateMapTerm, if (isFinal) sorterTerm else null)

    // --------------------------------------------------------------------------------------------
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
         |${ctx.reuseInputUnboxingCode(Set(inputTerm))}
         | // project key from input
         |$keyProjectionCode
         | // look up output buffer using current group key
         |$lookupInfo = $aggregateMapTerm.lookup($currentKeyTerm);
         |$currentAggBufferTerm = $lookupInfo.getValue();
         |
         |if (!$lookupInfo.isFound()) {
         |  $lazyInitAggBufferCode
         |  // append empty agg buffer into aggregate map for current group key
         |  try {
         |    $currentAggBufferTerm =
         |      $aggregateMapTerm.append($lookupInfo, ${initedAggBuffer.resultTerm});
         |  } catch (java.io.EOFException exp) {
         |    $dealWithAggHashMapOOM
         |  }
         |}
         | // aggregate buffer fields access
         |${ctx.reuseInputUnboxingCode(Set(currentAggBufferTerm))}
         | // do aggregate and update agg buffer
         |${aggregate.code}
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
      s"$outputResultFromMap"
    }

    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }
}
