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

package org.apache.flink.table.codegen.operator

import org.apache.flink.metrics.Gauge
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataTypes, DateType, RowType, TimestampType}
import org.apache.flink.table.codegen.CodeGenUtils.{baseRowFieldReadAccess, newName}
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator._
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedJoinConditionFunction}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.TwoInputSubstituteStreamOperator
import org.apache.flink.table.runtime.join.batch.HashJoinType
import org.apache.flink.table.runtime.join.batch.hashtable.longtable.{LongHashPartition, LongHybridHashTable}
import org.apache.flink.table.typeutils.{BaseRowSerializer, BinaryRowSerializer}

object LongHashJoinGenerator {

  def support(
      joinType: HashJoinType,
      keyType: RowType,
      filterNulls: Array[Boolean]): Boolean = {
    (joinType == HashJoinType.INNER ||
        joinType == HashJoinType.SEMI ||
        joinType == HashJoinType.ANTI ||
        joinType == HashJoinType.PROBE_OUTER) &&
        filterNulls.forall(b => b) &&
        keyType.getFieldTypes.length == 1 && {
      val t = keyType.getFieldTypes()(0)
      t == DataTypes.LONG || t == DataTypes.INT || t == DataTypes.SHORT || t == DataTypes.BYTE ||
          t == DataTypes.FLOAT || t == DataTypes.DOUBLE ||
          t.isInstanceOf[DateType] || t.isInstanceOf[TimestampType] || t == DataTypes.TIME
      // TODO decimal and multiKeys support.
    }
  }

  private def genGetLongKey(
      ctx: CodeGeneratorContext,
      keyType: RowType,
      keyMapping: Array[Int],
      rowTerm: String): String = {
    val singleType = keyType.getFieldTypes()(0).toInternalType
    val getCode = baseRowFieldReadAccess(ctx, keyMapping(0), rowTerm, singleType)
    val term = singleType match {
      case DataTypes.FLOAT => s"Float.floatToIntBits($getCode)"
      case DataTypes.DOUBLE => s"Double.doubleToLongBits($getCode)"
      case _ => getCode
    }
    s"return $term;"
  }

  def genAnyNullsInKeys(keyMapping: Array[Int], rowTerm: String): (String, String) = {
    val builder = new StringBuilder()
    val anyNullTerm = newName("anyNull")
    keyMapping.foreach(key =>
      builder.append(s"$anyNullTerm |= $rowTerm.isNullAt($key);")
    )
    (s"""
       |boolean $anyNullTerm = false;
       |$builder
     """.stripMargin, anyNullTerm)
  }

  def gen(
      conf: TableConfig,
      hashJoinType: HashJoinType,
      keyType: RowType,
      buildType: RowType,
      probeType: RowType,
      buildKeyMapping: Array[Int],
      probeKeyMapping: Array[Int],
      managedMemorySize: Long,
      preferredMemorySize: Long,
      perRequestSize: Long,
      buildRowSize: Int,
      buildRowCount: Long,
      reverseJoinFunction: Boolean,
      condFunc: GeneratedJoinConditionFunction)
    : TwoInputSubstituteStreamOperator[BaseRow, BaseRow, BaseRow] = {

    val buildSer = new BinaryRowSerializer(buildType.getFieldInternalTypes: _*)
    val probeSer = new BinaryRowSerializer(probeType.getFieldInternalTypes: _*)

    val tableTerm = newName("LongHashTable")
    val ctx = CodeGeneratorContext(conf, supportReference = true)
    val buildSerTerm = ctx.addReusableObject(buildSer, "buildSer")
    val probeSerTerm = ctx.addReusableObject(probeSer, "probeSer")

    val bGenProj = BaseRowSerializer.genProjection(buildSer.getTypes)
    ctx.addReusableInnerClass(bGenProj.name, bGenProj.code)
    val pGenProj = BaseRowSerializer.genProjection(probeSer.getTypes)
    ctx.addReusableInnerClass(pGenProj.name, pGenProj.code)
    ctx.addReusableInnerClass(condFunc.name, condFunc.code)

    ctx.addReusableMember(
      s"${bGenProj.name} buildToBinaryRow;",
      s"buildToBinaryRow = new ${bGenProj.name}();")
    ctx.addReusableMember(
      s"${pGenProj.name} probeToBinaryRow;",
      s"probeToBinaryRow = new ${pGenProj.name}();")
    ctx.addReusableMember(
      s"${condFunc.name} condFunc;",
      s"condFunc = new ${condFunc.name}();")
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
         |    super(getSqlConf(), getContainingTask(), $buildSerTerm, $probeSerTerm,
         |      getContainingTask().getEnvironment().getMemoryManager(),
         |      ${managedMemorySize}L, ${preferredMemorySize}L, ${perRequestSize}L,
         |      getContainingTask().getEnvironment().getIOManager(),
         |      $buildRowSize,
         |      ${buildRowCount}L / getRuntimeContext().getNumberOfParallelSubtasks());
         |  }
         |
         |  @Override
         |  public long getBuildLongKey($BASE_ROW row) {
         |    ${genGetLongKey(ctx, keyType, buildKeyMapping, "row")}
         |  }
         |
         |  @Override
         |  public long getProbeLongKey($BASE_ROW row) {
         |    ${genGetLongKey(ctx, keyType, probeKeyMapping, "row")}
         |  }
         |
         |  @Override
         |  public $BINARY_ROW probeToBinary($BASE_ROW row) {
         |    if (row instanceof $BINARY_ROW) {
         |      return ($BINARY_ROW) row;
         |    } else {
         |      return probeToBinaryRow.apply(row);
         |    }
         |  }
         |}
       """.stripMargin
    ctx.addReusableInnerClass(tableTerm, tableCode)

    ctx.addReusableNullRow("buildSideNullRow", buildSer.getNumFields)
    ctx.addOutputRecord(new RowType(), classOf[JoinedRow], "joinedRow")
    ctx.addReusableMember(s"$tableTerm table;")
    ctx.addReusableOpenStatement(s"table = new $tableTerm();")

    val (nullCheckBuildCode, nullCheckBuildTerm) = genAnyNullsInKeys(buildKeyMapping, "row")
    val (nullCheckProbeCode, nullCheckProbeTerm) = genAnyNullsInKeys(probeKeyMapping, "row")

    def collectCode(term1: String, term2: String): String =
      if (reverseJoinFunction) {
        generatorCollect(s"joinedRow.replace($term2, $term1)")
      } else {
        generatorCollect(s"joinedRow.replace($term1, $term2)")
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
           |    ${generatorCollect("probeRow")}
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
           |  ${generatorCollect("probeRow")}
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
           |  ${generatorCollect("row")}
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
         |  $BASE_ROW probeRow = table.getCurrentProbeRow();
         |  if (probeRow == null) {
         |    throw new RuntimeException("ProbeRow should not be null");
         |  }
         |  $joinCode
         |}
       """.stripMargin)

    ctx.addReusableCloseStatement(
      s"""
         |if (this.table != null) {
         |  this.table.close();
         |  this.table.free();
         |  this.table = null;
         |}
       """.stripMargin)

    val genOp = OperatorCodeGenerator.generateTwoInputStreamOperator(
      ctx,
      "LongHashJoinOperator",
      s"return $FIRST;",
      s"""
         |$BASE_ROW row = ($BASE_ROW) element.getValue();
         |$nullCheckBuildCode
         |if (!$nullCheckBuildTerm) {
         |  table.putBuildRow(row instanceof $BINARY_ROW ?
         |    ($BINARY_ROW) row : buildToBinaryRow.apply(row));
         |}
         |return $FIRST;
       """.stripMargin,
      s"""
         |LOG.info("Finish build phase.");
         |table.endBuild();
       """.stripMargin,
      s"""
         |$BASE_ROW row = ($BASE_ROW) element.getValue();
         |$nullCheckProbeCode
         |if (!$nullCheckProbeTerm) {
         |  if (table.tryProbe(row)) {
         |    joinWithNextKey();
         |  }
         |}
         |$nullOuterJoin
         |return $SECOND;
       """.stripMargin,
      s"""
         |LOG.info("Finish probe phase.");
         |while (this.table.nextMatching()) {
         |  joinWithNextKey();
         |}
         |LOG.info("Finish rebuild phase.");
       """.stripMargin,
      buildType,
      probeType)

    new TwoInputSubstituteStreamOperator[BaseRow, BaseRow, BaseRow](
      genOp.name, genOp.code, references = ctx.references)
  }
}
