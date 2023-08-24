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

import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression, GenerateUtils}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{fieldIndices, getReuseRowFieldExprs, newName, newNames, primitiveDefaultValue, primitiveTypeTermForType, BINARY_ROW, ROW_DATA}
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator.{genGetLongKey, genProjection}
import org.apache.flink.table.planner.plan.fusion.{OpFusionCodegenSpecBase, OpFusionContext}
import org.apache.flink.table.planner.plan.fusion.FusionCodegenUtil.{constructDoConsumeCode, constructDoConsumeFunction, evaluateRequiredVariables, extractRefInputFields}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.hashtable.LongHybridHashTable
import org.apache.flink.table.runtime.operators.join.{FlinkJoinType, HashJoinType}
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
import org.apache.flink.table.runtime.util.RowIterator
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.rex.RexNode

import java.util

/** Base operator fusion codegen spec for HashJoin. */
class HashJoinFusionCodegenSpec(
    opCodegenCtx: CodeGeneratorContext,
    isBroadcast: Boolean,
    leftIsBuild: Boolean,
    joinSpec: JoinSpec,
    estimatedLeftAvgRowSize: Int,
    estimatedRightAvgRowSize: Int,
    estimatedLeftRowCount: Long,
    estimatedRightRowCount: Long,
    compressionEnabled: Boolean,
    compressionBlockSize: Int)
  extends OpFusionCodegenSpecBase(opCodegenCtx) {

  private lazy val joinType: FlinkJoinType = joinSpec.getJoinType
  private lazy val hashJoinType: HashJoinType = HashJoinType.of(
    leftIsBuild,
    joinType.isLeftOuter,
    joinType.isRightOuter,
    joinType == FlinkJoinType.SEMI,
    joinType == FlinkJoinType.ANTI)
  private lazy val (buildKeys, probeKeys) = if (leftIsBuild) {
    (joinSpec.getLeftKeys, joinSpec.getRightKeys)
  } else {
    (joinSpec.getRightKeys, joinSpec.getLeftKeys)
  }
  private lazy val (buildRowSize, buildRowCount) = if (leftIsBuild) {
    (estimatedLeftAvgRowSize, estimatedLeftRowCount)
  } else {
    (estimatedRightAvgRowSize, estimatedRightRowCount)
  }
  private lazy val buildInputId = if (leftIsBuild) {
    1
  } else {
    2
  }

  private lazy val Seq(buildToBinaryRow, probeToBinaryRow) =
    newNames("buildToBinaryRow", "probeToBinaryRow")

  private lazy val hashTableTerm: String = newName("hashTable")

  private var buildContext: OpFusionContext = _
  private var probeContext: OpFusionContext = _
  private var buildType: RowType = _
  private var probeType: RowType = _
  private var keyType: RowType = _
  private var consumeFunctionName: String = _

  override def setup(opFusionContext: OpFusionContext): Unit = {
    super.setup(opFusionContext)
    val inputContexts = toScala(fusionContext.getInputFusionContexts)
    assert(inputContexts.size == 2)
    if (leftIsBuild) {
      buildContext = inputContexts.head
      probeContext = inputContexts(1)
    } else {
      buildContext = inputContexts(1)
      probeContext = inputContexts.head
    }

    buildType = buildContext.getOutputType
    probeType = probeContext.getOutputType
    if (leftIsBuild) {
      keyType = RowType.of(joinSpec.getLeftKeys.map(idx => buildType.getTypeAt(idx)): _*)
    } else {
      keyType = RowType.of(joinSpec.getLeftKeys.map(idx => probeType.getTypeAt(idx)): _*)
    }
  }

  override def variablePrefix: String = if (isBroadcast) { "bhj" }
  else { "shj" }

  override protected def doProcessProduce(codegenCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildContext.processProduce(codegenCtx)
    probeContext.processProduce(codegenCtx)
  }

  override protected def doEndInputProduce(codegenCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildContext.endInputProduce(codegenCtx)
    probeContext.endInputProduce(codegenCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    // only probe side will call the consumeProcess method to consume the output record
    if (inputId == buildInputId) {
      codegenBuild(toScala(inputVars), row)
    } else {
      codegenProbe(toScala(inputVars), row)
    }
  }

  private def codegenBuild(
      inputVars: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    // initialize hash table related code
    codegenHashTable()

    val (nullCheckBuildCode, nullCheckBuildTerm) = {
      genAnyNullsInKeys(buildKeys, inputVars)
    }
    s"""
       |$nullCheckBuildCode
       |if (!$nullCheckBuildTerm) {
       |  ${row.code}
       |  $hashTableTerm.putBuildRow(($BINARY_ROW) ${row.resultTerm});
       |}
       """.stripMargin
  }

  private def codegenProbe(
      inputVars: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(probeKeys, inputVars)
    val (processCode, buildIterTerm) = codegenProbeProcessCode(inputVars)
    s"""
       |// generate join key for probe side
       |${keyEv.code}
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
       |
       |if(!$anyNull && $buildIterTerm == null) {
       |   ${row.code}
       |   $hashTableTerm.insertIntoProbeBuffer(${row.resultTerm});
       |} else {
       |  $processCode
       |}
           """.stripMargin
  }

  override def doEndInputConsume(inputId: Int): String = {
    // If the hash table spill to disk during runtime, the probe endInput also need to
    // consumeProcess to consume the spilled record
    if (inputId == buildInputId) {
      s"""
         |LOG.info("Finish build phase.");
         |$hashTableTerm.endBuild();
       """.stripMargin
    } else {
      s"""
         |// Process the spilled partitions first
         |$codegenEndInputCode
         |${fusionContext.endInputConsume()}
         |""".stripMargin
    }
  }

  private def codegenEndInputCode(): String = {
    val spilledProbeRowTerm = newName("spilledProbeRow")
    if (buildInputId == 2) {
      getExprCodeGenerator.bindInput(probeType, spilledProbeRowTerm)
    } else {
      getExprCodeGenerator.bindSecondInput(probeType, spilledProbeRowTerm)
    }
    opCodegenCtx.startNewLocalVariableStatement(spilledProbeRowTerm)
    val inputVars = getReuseRowFieldExprs(opCodegenCtx, probeType, spilledProbeRowTerm)
    val (processCode, buildIterTerm) = codegenProbeProcessCode(inputVars)
    s"""
       |LOG.info("Finish probe phase.");
       |${opCodegenCtx.reuseLocalVariableCode(spilledProbeRowTerm)}
       |while ($hashTableTerm.nextMatching()) {
       |  ${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm =
       |      $hashTableTerm.getBuildSideIterator();
       |  $ROW_DATA $spilledProbeRowTerm = $hashTableTerm.getCurrentProbeRow();
       |  if ($spilledProbeRowTerm == null) {
       |    throw new RuntimeException("ProbeRow should not be null");
       |  }
       |  $processCode
       |}
       |LOG.info("Finish rebuild phase.");
       |
       |if(!$hashTableTerm.getPartitionsPendingForSMJ().isEmpty()) {
       |  throw new UnsupportedOperationException("Currently doesn't support fallback to sort merge join for hash join when 'table.exec.operator-fusion-codegen.enabled' is true.");
       |}
       |""".stripMargin
  }

  private def codegenProbeProcessCode(inputVars: Seq[GeneratedExpression]): (String, String) = {
    hashJoinType match {
      case HashJoinType.INNER =>
        codegenInnerProcessCode(inputVars)
      case HashJoinType.PROBE_OUTER => codegenProbeOuterProcessCode(inputVars)
      case HashJoinType.SEMI => codegenSemiProcessCode(inputVars)
      case HashJoinType.ANTI => codegenAntiProcessCode(inputVars)
      case _ =>
        throw new UnsupportedOperationException(
          s"Operator fusion codegen doesn't support $hashJoinType join now.")
    }
  }

  private def codegenInnerProcessCode(inputVars: Seq[GeneratedExpression]): (String, String) = {
    val (matched, checkCondition, buildLocalVars, buildVars) =
      getJoinCondition(inputVars, buildType)
    val resultVars = if (leftIsBuild) {
      buildVars ++ inputVars
    } else {
      inputVars ++ buildVars
    }
    val buildIterTerm = newName("buildIter")
    val processCode =
      s"""
         |if ($buildIterTerm != null ) {
         |  $buildLocalVars
         |  while ($buildIterTerm.advanceNext()) {
         |    $ROW_DATA $matched = $buildIterTerm.getRow();
         |    $checkCondition {
         |      ${codegenConsumeCode(resultVars)}
         |    }
         |  }
         |}
         |""".stripMargin
    (processCode, buildIterTerm)
  }

  private def codegenProbeOuterProcessCode(
      inputVars: Seq[GeneratedExpression]): (String, String) = {
    val matched = newName("buildRow")
    // start new local variable
    opCodegenCtx.startNewLocalVariableStatement(matched)
    val buildVars = genBuildSideVars(opCodegenCtx, matched, buildType)

    // filter the output via condition
    val conditionPassed = newName("conditionPassed")
    val checkCondition = if (joinSpec.getNonEquiCondition.isPresent) {
      // here need bind the buildRow before generate build condition
      if (buildInputId == 1) {
        getExprCodeGenerator.bindInput(buildType, matched)
      } else {
        getExprCodeGenerator.bindSecondInput(buildType, matched)
      }
      // generate the expr code
      val joinCondition = joinSpec.getNonEquiCondition.get
      // evaluate the variables from probe and build side that used by condition
      val evalCode = evaluateVarUsedInCondition(joinCondition, inputVars, buildVars)
      val expr = getExprCodeGenerator.generateExpression(joinCondition)
      s"""
         |boolean $conditionPassed = true;
         |$evalCode
         |if ($matched != null) {
         |  ${expr.code}
         |  $conditionPassed = !${expr.nullTerm} && ${expr.resultTerm};
         |}
       """.stripMargin
    } else {
      s"final boolean $conditionPassed = true;"
    }

    val resultVars = if (leftIsBuild) {
      buildVars ++ inputVars
    } else {
      inputVars ++ buildVars
    }
    val buildIterTerm = newName("buildIter")
    val found = newName("found")
    val hasNext = newName("hasNext")
    val processCode =
      s"""
         |boolean $found = false;
         |boolean $hasNext = false;
         |${opCodegenCtx.reuseLocalVariableCode(matched)}
         |while (($buildIterTerm != null && ($hasNext = $buildIterTerm.advanceNext())) || !$found) {
         |  $ROW_DATA $matched = $buildIterTerm != null && $hasNext ? $buildIterTerm.getRow() 
         |  : null;
         |  ${checkCondition.trim}
         |  if ($conditionPassed) {
         |    $found = true;
         |    ${codegenConsumeCode(resultVars)}
         |  }
         |}
         |""".stripMargin

    (processCode, buildIterTerm)
  }

  private def codegenSemiProcessCode(inputVars: Seq[GeneratedExpression]): (String, String) = {
    val (matched, checkCondition, buildLocalVars, _) = getJoinCondition(inputVars, buildType)

    val buildIterTerm = newName("buildIter")
    val processCode =
      s"""
         |if ($buildIterTerm != null ) {
         |  $buildLocalVars
         |  while ($buildIterTerm.advanceNext()) {
         |    $ROW_DATA $matched = $buildIterTerm.getRow();
         |    $checkCondition {
         |      ${codegenConsumeCode(inputVars)}
         |      break;
         |    }
         |  }
         |}
         |""".stripMargin

    (processCode, buildIterTerm)
  }

  private def codegenAntiProcessCode(inputVars: Seq[GeneratedExpression]): (String, String) = {
    val (matched, checkCondition, buildLocalVars, _) = getJoinCondition(inputVars, buildType)

    val buildIterTerm = newName("buildIter")
    val found = newName("found")
    val processCode =
      s"""
         |boolean $found = false;
         |if ($buildIterTerm != null ) {
         |  $buildLocalVars
         |  while ($buildIterTerm.advanceNext()) {
         |    $ROW_DATA $matched = $buildIterTerm.getRow();
         |    $checkCondition {
         |      $found = true;
         |      break;
         |    }
         |  }
         |}
         |
         |if (!$found) {
         |  ${codegenConsumeCode(inputVars)}
         |}
         |""".stripMargin

    (processCode, buildIterTerm)
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key has
   * any null in it or not.
   */
  private def genStreamSideJoinKey(
      probeKeyMapping: Array[Int],
      inputVars: Seq[GeneratedExpression]): (GeneratedExpression, String) = {
    // current only support one join key which is long type
    if (probeKeyMapping.length == 1) {
      // generate the join key as Long
      val ev = inputVars(probeKeyMapping(0))
      (ev, ev.nullTerm)
    } else {
      // generate the join key as BinaryRowData
      throw new UnsupportedOperationException(
        s"Operator fusion codegen doesn't support multiple join keys now.")
    }
  }

  private def genAnyNullsInKeys(
      keyMapping: Array[Int],
      input: Seq[GeneratedExpression]): (String, String) = {
    val builder = new StringBuilder
    val codeBuilder = new StringBuilder
    val anyNullTerm = newName("anyNull")

    keyMapping.foreach(
      key => {
        codeBuilder.append(input(key).code + "\n")
        builder.append(s"$anyNullTerm |= ${input(key).nullTerm};")
      })
    (
      s"""
         |boolean $anyNullTerm = false;
         |$codeBuilder
         |$builder
     """.stripMargin,
      anyNullTerm)
  }

  private def getJoinCondition(
      probeVars: Seq[GeneratedExpression],
      buildType: RowType): (String, String, String, Seq[GeneratedExpression]) = {
    val buildRow = newName("buildRow")
    // here need bind the buildRow before generate build condition
    if (buildInputId == 1) {
      getExprCodeGenerator.bindInput(buildType, buildRow)
    } else {
      getExprCodeGenerator.bindSecondInput(buildType, buildRow)
    }

    opCodegenCtx.startNewLocalVariableStatement(buildRow)
    val buildVars = genBuildSideVars(opCodegenCtx, buildRow, buildType)
    val checkCondition = if (joinSpec.getNonEquiCondition.isPresent) {
      val joinCondition = joinSpec.getNonEquiCondition.get
      // evaluate the variables from probe and build side that used by condition
      val eval = evaluateVarUsedInCondition(joinCondition, probeVars, buildVars)
      val expr = getExprCodeGenerator.generateExpression(joinCondition)
      val skipRow = s"${expr.nullTerm} || !${expr.resultTerm}"
      s"""
         |$eval
         |// generate join condition
         |${expr.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    val buildLocalVars =
      if (hashJoinType.leftSemiOrAnti() && !joinSpec.getNonEquiCondition.isPresent) {
        ""
      } else {
        opCodegenCtx.reuseLocalVariableCode(buildRow)
      }

    (buildRow, checkCondition, buildLocalVars, buildVars)
  }

  /** Generates the input row variables expr. */
  private def genBuildSideVars(
      ctx: CodeGeneratorContext,
      buildRow: String,
      buildType: RowType): Seq[GeneratedExpression] = {
    fieldIndices(buildType)
      .map(
        index => {
          var expr = GenerateUtils.generateFieldAccess(opCodegenCtx, buildType, buildRow, index)

          if (hashJoinType == HashJoinType.PROBE_OUTER) {
            val fieldType = buildType.getTypeAt(index).copy(true)
            val resultTypeTerm = primitiveTypeTermForType(fieldType)
            val defaultValue = primitiveDefaultValue(fieldType)
            val Seq(fieldTerm, nullTerm) =
              opCodegenCtx
                .addReusableLocalVariables((resultTypeTerm, "field"), ("boolean", "isNull"))
            val code = s"""
                          |$nullTerm = true;
                          |$fieldTerm = $defaultValue;
                          |if ($buildRow != null) {
                          |  ${expr.code}
                          |  $nullTerm = ${expr.nullTerm};
                          |  $fieldTerm = ${expr.resultTerm};
                          |}
          """.stripMargin
            expr = GeneratedExpression(fieldTerm, nullTerm, code, fieldType)
          }
          // bind the field expr to ctx to reuse when generate join condition code
          ctx
            .addReusableInputUnboxingExprs(buildRow, index, expr)

          expr
        })
      .toSeq
  }

  private def evaluateVarUsedInCondition(
      joinCondition: RexNode,
      probeVars: Seq[GeneratedExpression],
      buildVars: Seq[GeneratedExpression]): String = {
    val (buildIndices, probeIndices) = if (buildInputId == 1) {
      extractRefInputFields(Seq(joinCondition), buildType, probeType)
    } else {
      val (input1Indices, input2Indices) =
        extractRefInputFields(Seq(joinCondition), probeType, buildType)
      (input2Indices, input1Indices)
    }

    s"""
       |${evaluateRequiredVariables(probeVars, probeIndices)}
       |${evaluateRequiredVariables(buildVars, buildIndices)}
       |""".stripMargin
  }

  override def usedInputColumns(inputId: Int): util.Set[Integer] = {
    val set: util.Set[Integer] = new util.HashSet[Integer]()
    if (inputId == buildInputId) {
      buildKeys.toStream.map(key => set.add(key))
    } else {
      probeKeys.toStream.map(key => set.add(key))
    }
    set
  }

  override def getInputRowDataClass(inputId: Int): Class[_ <: RowData] = {
    // Build and probe side both wrap to BinaryRowData. For probe side,
    // shuffle hash join maybe need to spill to disk if data skew.
    classOf[BinaryRowData]
  }

  private def codegenConsumeCode(resultVars: Seq[GeneratedExpression]): String = {
    // Here need to cache to avoid generating the consume code multiple time
    if (consumeFunctionName == null) {
      consumeFunctionName = constructDoConsumeFunction(
        variablePrefix,
        opCodegenCtx,
        fusionContext,
        fusionContext.getOutputType)
    }
    constructDoConsumeCode(consumeFunctionName, resultVars)
  }

  private def codegenHashTable(): Unit = {
    val buildSer = new BinaryRowDataSerializer(buildType.getFieldCount)
    val buildSerTerm = opCodegenCtx.addReusableObject(buildSer, "buildSer")
    val probeSer = new BinaryRowDataSerializer(probeType.getFieldCount)
    val probeSerTerm = opCodegenCtx.addReusableObject(probeSer, "probeSer")

    val bGenProj =
      genProjection(
        opCodegenCtx.tableConfig,
        opCodegenCtx.classLoader,
        buildType.getChildren.toArray(Array[LogicalType]()))
    opCodegenCtx.addReusableInnerClass(bGenProj.getClassName, bGenProj.getCode)
    val pGenProj =
      genProjection(
        opCodegenCtx.tableConfig,
        opCodegenCtx.classLoader,
        probeType.getChildren.toArray(Array[LogicalType]()))
    opCodegenCtx.addReusableInnerClass(pGenProj.getClassName, pGenProj.getCode)

    opCodegenCtx.addReusableMember(s"${bGenProj.getClassName} $buildToBinaryRow;")
    val buildProjRefs = opCodegenCtx.addReusableObject(bGenProj.getReferences, "buildProjRefs")
    opCodegenCtx.addReusableInitStatement(
      s"$buildToBinaryRow = new ${bGenProj.getClassName}($buildProjRefs);")

    opCodegenCtx.addReusableMember(s"${pGenProj.getClassName} $probeToBinaryRow;")
    val probeProjRefs = opCodegenCtx.addReusableObject(pGenProj.getReferences, "probeProjRefs")
    opCodegenCtx.addReusableInitStatement(
      s"$probeToBinaryRow = new ${pGenProj.getClassName}($probeProjRefs);")

    val hashTableClassTerm = newName("LongHashTable")
    val tableCode =
      s"""
         |public class $hashTableClassTerm extends ${classOf[LongHybridHashTable].getCanonicalName} {
         |
         |  public $hashTableClassTerm(long memorySize) {
         |    super(getContainingTask(),
         |      $compressionEnabled, $compressionBlockSize,
         |      $buildSerTerm, $probeSerTerm,
         |      getContainingTask().getEnvironment().getMemoryManager(),
         |      memorySize,
         |      getContainingTask().getEnvironment().getIOManager(),
         |      $buildRowSize,
         |      ${buildRowCount}L / getRuntimeContext().getNumberOfParallelSubtasks());
         |  }
         |
         |  @Override
         |  public long getBuildLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, buildKeys, "row")}
         |  }
         |
         |  @Override
         |  public long getProbeLongKey($ROW_DATA row) {
         |    ${genGetLongKey(keyType, probeKeys, "row")}
         |  }
         |
         |  @Override
         |  public $BINARY_ROW probeToBinary($ROW_DATA row) {
         |    if (row instanceof $BINARY_ROW) {
         |      return ($BINARY_ROW) row;
         |    } else {
         |      return $probeToBinaryRow.apply(row);
         |    }
         |  }
         |}
       """.stripMargin
    opCodegenCtx.addReusableInnerClass(hashTableClassTerm, tableCode)
    opCodegenCtx.addReusableMember(s"$hashTableClassTerm $hashTableTerm;")
    val memorySizeTerm = newName("memorySize")
    opCodegenCtx.addReusableOpenStatement(
      s"long $memorySizeTerm = computeMemorySize(${fusionContext.getManagedMemoryFraction});")
    opCodegenCtx.addReusableOpenStatement(
      s"$hashTableTerm = new $hashTableClassTerm($memorySizeTerm);")

    opCodegenCtx.addReusableCloseStatement(s"""
                                              |if (this.$hashTableTerm != null) {
                                              |  this.$hashTableTerm.close();
                                              |  this.$hashTableTerm.free();
                                              |  this.$hashTableTerm = null;
                                              |}
       """.stripMargin)
  }
}
