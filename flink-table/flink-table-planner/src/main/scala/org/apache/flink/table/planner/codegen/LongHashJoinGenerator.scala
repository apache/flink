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

import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.metrics.Gauge
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{generateCollect, INPUT_SELECTION}
import org.apache.flink.table.runtime.generated.{GeneratedJoinCondition, GeneratedProjection}
import org.apache.flink.table.runtime.hashtable.{LongHashPartition, LongHybridHashTable, ProbeIterator}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.operators.join.{HashJoinType, SortMergeJoinFunction}
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
import org.apache.flink.table.runtime.util.{RowIterator, StreamRecordCollector}
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.LogicalTypeRoot._

/** Generate a long key hash join operator using [[LongHybridHashTable]]. */
object LongHashJoinGenerator {

  def support(joinType: HashJoinType, keyType: RowType, filterNulls: Array[Boolean]): Boolean = {
    (joinType == HashJoinType.INNER ||
      joinType == HashJoinType.SEMI ||
      joinType == HashJoinType.ANTI ||
      joinType == HashJoinType.PROBE_OUTER) &&
    filterNulls.forall(b => b) &&
    keyType.getFieldCount == 1 && {
      keyType.getTypeAt(0).getTypeRoot match {
        case BIGINT | INTEGER | SMALLINT | TINYINT | FLOAT | DOUBLE | DATE |
            TIME_WITHOUT_TIME_ZONE =>
          true
        case TIMESTAMP_WITHOUT_TIME_ZONE =>
          val timestampType = keyType.getTypeAt(0).asInstanceOf[TimestampType]
          TimestampData.isCompact(timestampType.getPrecision)
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          val lzTs = keyType.getTypeAt(0).asInstanceOf[LocalZonedTimestampType]
          TimestampData.isCompact(lzTs.getPrecision)
        case _ => false
      }
      // TODO decimal and multiKeys support.
      // TODO All HashJoinType support.
    }
  }

  def genGetLongKey(keyType: RowType, keyMapping: Array[Int], rowTerm: String): String = {
    val singleType = keyType.getTypeAt(0)
    val getCode = rowFieldReadAccess(keyMapping(0), rowTerm, singleType)
    val term = singleType.getTypeRoot match {
      case FLOAT => s"Float.floatToIntBits($getCode)"
      case DOUBLE => s"Double.doubleToLongBits($getCode)"
      case TIMESTAMP_WITHOUT_TIME_ZONE => s"$getCode.getMillisecond()"
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE => s"$getCode.getMillisecond()"
      case _ => getCode
    }
    s"return $term;"
  }

  def genAnyNullsInKeys(keyMapping: Array[Int], rowTerm: String): (String, String) = {
    val builder = new StringBuilder()
    val anyNullTerm = newName("anyNull")
    keyMapping.foreach(key => builder.append(s"$anyNullTerm |= $rowTerm.isNullAt($key);"))
    (
      s"""
         |boolean $anyNullTerm = false;
         |$builder
     """.stripMargin,
      anyNullTerm)
  }

  def genProjection(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      types: Array[LogicalType]): GeneratedProjection = {
    val rowType = RowType.of(types: _*)
    ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(tableConfig, classLoader),
      "Projection",
      rowType,
      rowType,
      types.indices.toArray)
  }

  def gen(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      hashJoinType: HashJoinType,
      keyType: RowType,
      buildType: RowType,
      probeType: RowType,
      buildKeyMapping: Array[Int],
      probeKeyMapping: Array[Int],
      buildRowSize: Int,
      buildRowCount: Long,
      reverseJoinFunction: Boolean,
      condFunc: GeneratedJoinCondition,
      leftIsBuild: Boolean,
      compressionEnabled: Boolean,
      compressionBlockSize: Int,
      sortMergeJoinFunction: SortMergeJoinFunction): CodeGenOperatorFactory[RowData] = {

    val buildSer = new BinaryRowDataSerializer(buildType.getFieldCount)
    val probeSer = new BinaryRowDataSerializer(probeType.getFieldCount)

    val tableTerm = newName("LongHashTable")
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val buildSerTerm = ctx.addReusableObject(buildSer, "buildSer")
    val probeSerTerm = ctx.addReusableObject(probeSer, "probeSer")

    val bGenProj =
      genProjection(tableConfig, classLoader, buildType.getChildren.toArray(Array[LogicalType]()))
    ctx.addReusableInnerClass(bGenProj.getClassName, bGenProj.getCode)
    val pGenProj =
      genProjection(tableConfig, classLoader, probeType.getChildren.toArray(Array[LogicalType]()))
    ctx.addReusableInnerClass(pGenProj.getClassName, pGenProj.getCode)
    ctx.addReusableInnerClass(condFunc.getClassName, condFunc.getCode)

    ctx.addReusableMember(s"${bGenProj.getClassName} buildToBinaryRow;")
    val buildProjRefs = ctx.addReusableObject(bGenProj.getReferences, "buildProjRefs")
    ctx.addReusableInitStatement(
      s"buildToBinaryRow = new ${bGenProj.getClassName}($buildProjRefs);")

    ctx.addReusableMember(s"${pGenProj.getClassName} probeToBinaryRow;")
    val probeProjRefs = ctx.addReusableObject(pGenProj.getReferences, "probeProjRefs")
    ctx.addReusableInitStatement(
      s"probeToBinaryRow = new ${pGenProj.getClassName}($probeProjRefs);")

    ctx.addReusableMember(s"${condFunc.getClassName} condFunc;")
    val condRefs = ctx.addReusableObject(condFunc.getReferences, "condRefs")
    ctx.addReusableInitStatement(s"condFunc = new ${condFunc.getClassName}($condRefs);")
    ctx.addReusableOpenStatement(s"condFunc.setRuntimeContext(getRuntimeContext());")
    ctx.addReusableOpenStatement(s"condFunc.open(new ${className[Configuration]}());")
    ctx.addReusableCloseStatement(s"condFunc.close();")

    val leftIsBuildTerm = newName("leftIsBuild")
    ctx.addReusableMember(s"private final boolean $leftIsBuildTerm = $leftIsBuild;")

    val smjFunctionTerm = className[SortMergeJoinFunction]
    ctx.addReusableMember(s"private final $smjFunctionTerm sortMergeJoinFunction;")
    val smjFunctionRefs = ctx.addReusableObject(Array(sortMergeJoinFunction), "smjFunctionRefs")
    ctx.addReusableInitStatement(s"sortMergeJoinFunction = $smjFunctionRefs[0];")

    val fallbackSMJ = newName("fallbackSMJ")
    ctx.addReusableMember(s"private transient boolean $fallbackSMJ = false;")

    val gauge = classOf[Gauge[_]].getCanonicalName
    ctx.addReusableOpenStatement(
      s"""
         |getMetricGroup().gauge("memoryUsedSizeInBytes", new $gauge<Long>() {
         |  @Override
         |  public Long getValue() {
         |    return table.getUsedMemoryInBytes();
         |  }
         |});
         |getMetricGroup().gauge("numSpillFiles", new $gauge<Long>() {
         |  @Override
         |  public Long getValue() {
         |    return table.getNumSpillFiles();
         |  }
         |});
         |getMetricGroup().gauge("spillInBytes", new $gauge<Long>() {
         |  @Override
         |  public Long getValue() {
         |    return table.getSpillInBytes();
         |  }
         |});
       """.stripMargin)

    val tableCode =
      s"""
         |public class $tableTerm extends ${classOf[LongHybridHashTable].getCanonicalName} {
         |
         |  public $tableTerm() {
         |    super(getContainingTask(),
         |      $compressionEnabled, $compressionBlockSize,
         |      $buildSerTerm, $probeSerTerm,
         |      getContainingTask().getEnvironment().getMemoryManager(),
         |      computeMemorySize(),
         |      getContainingTask().getEnvironment().getIOManager(),
         |      $buildRowSize,
         |      ${buildRowCount}L / getRuntimeContext().getNumberOfParallelSubtasks());
         |  }
         |
         |  @Override
         |  public long getBuildLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, buildKeyMapping, "row")}
         |  }
         |
         |  @Override
         |  public long getProbeLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, probeKeyMapping, "row")}
         |  }
         |
         |  @Override
         |  public $BINARY_ROW probeToBinary($ROW_DATA row) {
         |    if (row instanceof $BINARY_ROW) {
         |      return ($BINARY_ROW) row;
         |    } else {
         |      return probeToBinaryRow.apply(row);
         |    }
         |  }
         |}
       """.stripMargin
    ctx.addReusableInnerClass(tableTerm, tableCode)

    ctx.addReusableNullRow("buildSideNullRow", buildSer.getArity)
    ctx.addReusableOutputRecord(RowType.of(), classOf[JoinedRowData], "joinedRow")
    ctx.addReusableMember(s"$tableTerm table;")
    ctx.addReusableOpenStatement(s"table = new $tableTerm();")

    val (nullCheckBuildCode, nullCheckBuildTerm) = genAnyNullsInKeys(buildKeyMapping, "row")
    val (nullCheckProbeCode, nullCheckProbeTerm) = genAnyNullsInKeys(probeKeyMapping, "row")

    def collectCode(term1: String, term2: String) =
      if (reverseJoinFunction) {
        generateCollect(s"joinedRow.replace($term2, $term1)")
      } else {
        generateCollect(s"joinedRow.replace($term1, $term2)")
      }

    val applyCond =
      if (reverseJoinFunction) {
        s"condFunc.apply(probeRow, buildIter.getRow())"
      } else {
        s"condFunc.apply(buildIter.getRow(), probeRow)"
      }

    // innerJoin Now.
    val joinCode = hashJoinType match {
      case HashJoinType.INNER =>
        s"""
           |while (buildIter.advanceNext()) {
           |  if ($applyCond) {
           |    ${collectCode("buildIter.getRow()", "probeRow")}
           |  }
           |}
         """.stripMargin
      case HashJoinType.SEMI =>
        s"""
           |while (buildIter.advanceNext()) {
           |  if ($applyCond) {
           |    ${generateCollect("probeRow")}
           |    break;
           |  }
           |}
         """.stripMargin
      case HashJoinType.ANTI =>
        s"""
           |boolean matched = false;
           |while (buildIter.advanceNext()) {
           |  if ($applyCond) {
           |    matched = true;
           |    break;
           |  }
           |}
           |if (!matched) {
           |  ${generateCollect("probeRow")}
           |}
         """.stripMargin
      case HashJoinType.PROBE_OUTER =>
        s"""
           |boolean matched = false;
           |while (buildIter.advanceNext()) {
           |  if ($applyCond) {
           |    ${collectCode("buildIter.getRow()", "probeRow")}
           |    matched = true;
           |  }
           |}
           |if (!matched) {
           |  ${collectCode("buildSideNullRow", "probeRow")}
           |}
         """.stripMargin
    }

    val nullOuterJoin = hashJoinType match {
      case HashJoinType.ANTI =>
        s"""
           |else {
           |  ${generateCollect("row")}
           |}
         """.stripMargin
      case HashJoinType.PROBE_OUTER =>
        s"""
           |else {
           |  ${collectCode("buildSideNullRow", "row")}
           |}
         """.stripMargin
      case _ => ""
    }

    ctx.addReusableMember(
      s"""
         |private void joinWithNextKey() throws Exception {
         |  ${classOf[LongHashPartition#MatchIterator].getCanonicalName} buildIter =
         |      table.getBuildSideIterator();
         |  $ROW_DATA probeRow = table.getCurrentProbeRow();
         |  if (probeRow == null) {
         |    throw new RuntimeException("ProbeRow should not be null");
         |  }
         |  $joinCode
         |}
       """.stripMargin)

    // fallback to sort merge join in probe phase
    val rowIter = classOf[RowIterator[_]].getCanonicalName
    ctx.addReusableMember(s"""
                             |private void fallbackSMJProcessPartition() throws Exception {
                             |  if(!table.getPartitionsPendingForSMJ().isEmpty()) {
                             |    table.releaseMemoryCacheForSMJ();
                             |    LOG.info(
                             |    "Fallback to sort merge join to process spilled partitions.");
                             |    initialSortMergeJoinFunction();
                             |    $fallbackSMJ = true;
                             |
                             |    for(${classOf[LongHashPartition].getCanonicalName} p : 
                             |      table.getPartitionsPendingForSMJ()) {
                             |      $rowIter<$BINARY_ROW> buildSideIter = 
                             |      table.getSpilledPartitionBuildSideIter(p);
                             |      while (buildSideIter.advanceNext()) {
                             |        processSortMergeJoinElement1(buildSideIter.getRow());
                             |      }
                             |
                             |      ${classOf[ProbeIterator].getCanonicalName} probeIter =
                             |      table.getSpilledPartitionProbeSideIter(p);
                             |      $BINARY_ROW probeNext;
                             |      while ((probeNext = probeIter.next()) != null) {
                             |        processSortMergeJoinElement2(probeNext);
                             |      }
                             |    }
                             |
                             |    closeHashTable();
                             |
                             |    sortMergeJoinFunction.endInput(1);
                             |    sortMergeJoinFunction.endInput(2);
                             |    LOG.info("Finish sort merge join for spilled partitions.");
                             |  }
                             |}
       """.stripMargin)

    val collector = classOf[StreamRecordCollector[_]].getCanonicalName
    ctx.addReusableMember(s"""
                             |private void initialSortMergeJoinFunction() throws Exception {
                             |  sortMergeJoinFunction.open(
                             |    true,
                             |    this.getContainingTask(),
                             |    this.getOperatorConfig(),
                             |    new $collector<$ROW_DATA>(output),
                             |    this.computeMemorySize(),
                             |    this.getRuntimeContext(),
                             |    this.getMetricGroup());
                             |}
       """.stripMargin)

    ctx.addReusableMember(
      s"""
         |private void processSortMergeJoinElement1($ROW_DATA rowData) throws Exception {
         |  if($leftIsBuild) {
         |    sortMergeJoinFunction.processElement1(rowData);
         |  } else {
         |    sortMergeJoinFunction.processElement2(rowData);
         |  }
         |}
       """.stripMargin)

    ctx.addReusableMember(
      s"""
         |private void processSortMergeJoinElement2($ROW_DATA rowData) throws Exception {
         |  if($leftIsBuild) {
         |    sortMergeJoinFunction.processElement2(rowData);
         |  } else {
         |    sortMergeJoinFunction.processElement1(rowData);
         |  }
         |}
       """.stripMargin)

    ctx.addReusableMember(s"""
                             |private void closeHashTable() {
                             |  if (this.table != null) {
                             |    this.table.close();
                             |    this.table.free();
                             |    this.table = null;
                             |  }
                             |}
       """.stripMargin)

    ctx.addReusableCloseStatement(s"""
                                     |closeHashTable();
                                     |
                                     |if ($fallbackSMJ) {
                                     |  sortMergeJoinFunction.close();
                                     |}
       """.stripMargin)

    val buildEnd = newName("buildEnd")
    ctx.addReusableMember(s"private transient boolean $buildEnd = false;")

    val genOp = OperatorCodeGenerator.generateTwoInputStreamOperator[RowData, RowData, RowData](
      ctx,
      "LongHashJoinOperator",
      s"""
         |$ROW_DATA row = ($ROW_DATA) element.getValue();
         |$nullCheckBuildCode
         |if (!$nullCheckBuildTerm) {
         |  table.putBuildRow(row instanceof $BINARY_ROW ?
         |    ($BINARY_ROW) row : buildToBinaryRow.apply(row));
         |}
       """.stripMargin,
      s"""
         |$ROW_DATA row = ($ROW_DATA) element.getValue();
         |$nullCheckProbeCode
         |if (!$nullCheckProbeTerm) {
         |  if (table.tryProbe(row)) {
         |    joinWithNextKey();
         |  }
         |}
         |$nullOuterJoin
       """.stripMargin,
      buildType,
      probeType,
      nextSelectionCode = Some(s"""
                                  |if ($buildEnd) {
                                  |  return $INPUT_SELECTION.SECOND;
                                  |} else {
                                  |  return $INPUT_SELECTION.FIRST;
                                  |}
         """.stripMargin),
      endInputCode1 = Some(s"""
                              |LOG.info("Finish build phase.");
                              |table.endBuild();
                              |$buildEnd = true;
       """.stripMargin),
      endInputCode2 = Some(s"""
                              |LOG.info("Finish probe phase.");
                              |while (this.table.nextMatching()) {
                              |  joinWithNextKey();
                              |}
                              |LOG.info("Finish rebuild phase.");
                              |
                              |fallbackSMJProcessPartition();
         """.stripMargin)
    )

    new CodeGenOperatorFactory[RowData](genOp)
  }
}
