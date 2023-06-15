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
import org.apache.flink.table.planner.codegen.CodeGenUtils.{fieldIndices, newName, newNames, primitiveDefaultValue, primitiveTypeTermForType, BINARY_ROW, ROW_DATA}
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator.{genGetLongKey, genProjection}
import org.apache.flink.table.planner.plan.fusion.{OpFusionCodegenSpecBase, OpFusionCodegenSpecGenerator, OpFusionContext}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.runtime.hashtable.LongHybridHashTable
import org.apache.flink.table.runtime.operators.join.{FlinkJoinType, HashJoinType}
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
import org.apache.flink.table.runtime.util.RowIterator
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import java.util

/** Base operator fusion codegen spec for HashJoin. */
class HashJoinFusionCodegenSpec(
    operatorCtx: CodeGeneratorContext,
    isBroadcast: Boolean,
    leftIsBuild: Boolean,
    joinSpec: JoinSpec,
    estimatedLeftAvgRowSize: Int,
    estimatedRightAvgRowSize: Int,
    estimatedLeftRowCount: Long,
    estimatedRightRowCount: Long,
    compressionEnabled: Boolean,
    compressionBlockSize: Int)
  extends OpFusionCodegenSpecBase(operatorCtx) {

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

  private var buildInput: OpFusionCodegenSpecGenerator = _
  private var probeInput: OpFusionCodegenSpecGenerator = _
  private var buildType: RowType = _
  private var probeType: RowType = _
  private var keyType: RowType = _

  override def setup(opFusionContext: OpFusionContext): Unit = {
    super.setup(opFusionContext)
    val inputs = toScala(fusionContext.getInputs)
    assert(inputs.size == 2)
    if (leftIsBuild) {
      buildInput = inputs.head
      probeInput = inputs(1)
    } else {
      buildInput = inputs(1)
      probeInput = inputs.head
    }

    buildType = buildInput.getOutputType
    probeType = probeInput.getOutputType
    if (leftIsBuild) {
      keyType = RowType.of(joinSpec.getLeftKeys.map(idx => buildType.getTypeAt(idx)): _*)
    } else {
      keyType = RowType.of(joinSpec.getLeftKeys.map(idx => probeType.getTypeAt(idx)): _*)
    }
  }

  override def variablePrefix: String = if (isBroadcast) { "bhj" }
  else { "shj" }

  override protected def doProcessProduce(fusionCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildInput.processProduce(fusionCtx)
    probeInput.processProduce(fusionCtx)
  }

  override protected def doEndInputProduce(fusionCtx: CodeGeneratorContext): Unit = {
    // call build side first, then call probe side
    buildInput.endInputProduce(fusionCtx)
    probeInput.endInputProduce(fusionCtx)
  }

  override def doProcessConsume(
      inputId: Int,
      inputVars: util.List[GeneratedExpression],
      row: GeneratedExpression): String = {
    // only probe side will call the consumeProcess method to consume the output record
    if (inputId == buildInputId) {
      codegenBuild(toScala(inputVars), row)
    } else {
      codegenProbe(inputVars)
    }
  }

  private def codegenBuild(
      inputVars: Seq[GeneratedExpression],
      row: GeneratedExpression): String = {
    // initialize hash table related code
    if (isBroadcast) {
      codegenHashTable(false)
    } else {
      // TODO Shuffled HashJoin support build side spill to disk
      codegenHashTable(true)
    }

    val (nullCheckBuildCode, nullCheckBuildTerm) = {
      genAnyNullsInKeys(buildKeys, inputVars)
    }
    s"""
       |$nullCheckBuildCode
       |if (!$nullCheckBuildTerm) {
       |  ${row.getCode}
       |  $hashTableTerm.putBuildRow(($BINARY_ROW) ${row.resultTerm});
       |}
       """.stripMargin
  }

  private def codegenProbe(inputVars: util.List[GeneratedExpression]): String = {
    hashJoinType match {
      case HashJoinType.INNER =>
        codegenInnerProbe(inputVars)
      case HashJoinType.PROBE_OUTER => codegenProbeOuterProbe(inputVars)
      case HashJoinType.SEMI => codegenSemiProbe(inputVars)
      case HashJoinType.ANTI => codegenAntiProbe(inputVars)
      case _ =>
        throw new UnsupportedOperationException(
          s"Operator fusion codegen doesn't support $hashJoinType now.")
    }
  }

  private def codegenInnerProbe(inputVars: util.List[GeneratedExpression]): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(probeKeys, inputVars)
    val keyCode = keyEv.getCode
    val (matched, checkCondition, buildLocalVars, buildVars) = getJoinCondition(buildType)
    val resultVars = if (leftIsBuild) {
      buildVars ++ toScala(inputVars)
    } else {
      toScala(inputVars) ++ buildVars
    }
    val buildIterTerm = newName("buildIter")
    s"""
       |// generate join key for probe side
       |$keyCode
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
       |if ($buildIterTerm != null ) {
       |  $buildLocalVars
       |  while ($buildIterTerm.advanceNext()) {
       |    $ROW_DATA $matched = $buildIterTerm.getRow();
       |    $checkCondition {
       |      ${fusionContext.processConsume(toJava(resultVars))}
       |    }
       |  }
       |}
           """.stripMargin
  }

  private def codegenProbeOuterProbe(inputVars: util.List[GeneratedExpression]): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(probeKeys, inputVars)
    val keyCode = keyEv.getCode
    val matched = newName("buildRow")
    // start new local variable
    getOperatorCtx.startNewLocalVariableStatement(matched)
    val buildVars = genInputVars(matched, buildType)

    // filter the output via condition
    val conditionPassed = newName("conditionPassed")
    val checkCondition = if (joinSpec.getNonEquiCondition.isPresent) {
      // here need bind the buildRow before generate build condition
      if (buildInputId == 1) {
        getExprCodeGenerator.bindInput(buildType, matched)
      } else {
        getExprCodeGenerator.bindSecondInput(buildType, matched)
      }
      // TODO evaluate the variables from probe and build side that used by condition in advance
      // generate the expr code
      val expr = getExprCodeGenerator.generateExpression(joinSpec.getNonEquiCondition.get)
      s"""
         |boolean $conditionPassed = true;
         |if ($matched != null) {
         |  ${expr.getCode}
         |  $conditionPassed = !${expr.nullTerm} && ${expr.resultTerm};
         |}
       """.stripMargin
    } else {
      s"final boolean $conditionPassed = true;"
    }

    // generate the final result vars that need to consider the null for outer join
    val buildResultVars = genProbeOuterBuildVars(matched, buildVars)
    val resultVars = if (leftIsBuild) {
      buildResultVars ++ toScala(inputVars)
    } else {
      toScala(inputVars) ++ buildResultVars
    }
    val buildIterTerm = newName("buildIter")
    val found = newName("found")
    val hasNext = newName("hasNext")
    s"""
       |// generate join key for probe side
       |$keyCode
       |
       |boolean $found = false;
       |boolean $hasNext = false;
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
       |${getOperatorCtx.reuseLocalVariableCode(matched)}
       |while (($buildIterTerm != null && ($hasNext = $buildIterTerm.advanceNext())) || !$found) {
       |  $ROW_DATA $matched = $buildIterTerm != null && $hasNext ? $buildIterTerm.getRow() : null;
       |  ${checkCondition.trim}
       |  if ($conditionPassed) {
       |    $found = true;
       |    ${fusionContext.processConsume(toJava(resultVars))}
       |  }
       |}
           """.stripMargin
  }

  private def codegenSemiProbe(inputVars: util.List[GeneratedExpression]): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(probeKeys, inputVars)
    val keyCode = keyEv.getCode
    val (matched, checkCondition, buildLocalVars, _) = getJoinCondition(buildType)

    val buildIterTerm = newName("buildIter")
    s"""
       |// generate join key for probe side
       |$keyCode
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
       |if ($buildIterTerm != null ) {
       |  $buildLocalVars
       |  while ($buildIterTerm.advanceNext()) {
       |    $ROW_DATA $matched = $buildIterTerm.getRow();
       |    $checkCondition {
       |      ${fusionContext.processConsume(inputVars)}
       |      break;
       |    }
       |  }
       |}
           """.stripMargin
  }

  private def codegenAntiProbe(inputVars: util.List[GeneratedExpression]): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(probeKeys, inputVars)
    val keyCode = keyEv.getCode
    val (matched, checkCondition, buildLocalVars, _) = getJoinCondition(buildType)

    val buildIterTerm = newName("buildIter")
    val found = newName("found")

    s"""
       |// generate join key for probe side
       |$keyCode
       |boolean $found = false;
       |// find matches from hash table
       |${classOf[RowIterator[_]].getCanonicalName} $buildIterTerm = $anyNull ?
       |  null : $hashTableTerm.get(${keyEv.resultTerm});
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
       |  ${fusionContext.processConsume(inputVars)}
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
      fusionContext.endInputConsume()
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key has
   * any null in it or not.
   */
  protected def genStreamSideJoinKey(
      probeKeyMapping: Array[Int],
      inputVars: util.List[GeneratedExpression]): (GeneratedExpression, String) = {
    // current only support one join key which is long type
    if (probeKeyMapping.length == 1) {
      // generate the join key as Long
      val ev = inputVars.get(probeKeyMapping(0))
      (ev, ev.nullTerm)
    } else {
      // generate the join key as BinaryRowData
      throw new UnsupportedOperationException(
        s"Operator fusion codegen doesn't support multiple join keys now.")
    }
  }

  protected def genAnyNullsInKeys(
      keyMapping: Array[Int],
      input: Seq[GeneratedExpression]): (String, String) = {
    val builder = new StringBuilder
    val codeBuilder = new StringBuilder
    val anyNullTerm = newName("anyNull")

    keyMapping.foreach(
      key => {
        codeBuilder.append(input(key).getCode + "\n")
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

  protected def getJoinCondition(
      buildType: RowType): (String, String, String, Seq[GeneratedExpression]) = {
    val buildRow = newName("buildRow")
    // here need bind the buildRow before generate build condition
    if (buildInputId == 1) {
      getExprCodeGenerator.bindInput(buildType, buildRow)
    } else {
      getExprCodeGenerator.bindSecondInput(buildType, buildRow)
    }

    getOperatorCtx.startNewLocalVariableStatement(buildRow)
    val buildVars = genInputVars(buildRow, buildType)
    val checkCondition = if (joinSpec.getNonEquiCondition.isPresent) {
      // bind the build row name again
      val expr = getExprCodeGenerator.generateExpression(joinSpec.getNonEquiCondition.get)
      val skipRow = s"${expr.nullTerm} || !${expr.resultTerm}"
      s"""
         |// generate join condition
         |${expr.getCode}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    val buildLocalVars =
      if (hashJoinType.leftSemiOrAnti() && !joinSpec.getNonEquiCondition.isPresent) {
        ""
      } else {
        getOperatorCtx.reuseLocalVariableCode(buildRow)
      }

    (buildRow, checkCondition, buildLocalVars, buildVars)
  }

  /** Generates build side expr for outer join. */
  protected def genProbeOuterBuildVars(
      buildRow: String,
      buildVars: Seq[GeneratedExpression]): Seq[GeneratedExpression] = {
    buildVars.zipWithIndex.map {
      case (expr, i) =>
        val fieldType = buildType.getTypeAt(i)
        val resultTypeTerm = primitiveTypeTermForType(fieldType)
        val defaultValue = primitiveDefaultValue(fieldType)
        val Seq(fieldTerm, nullTerm) =
          getOperatorCtx.addReusableLocalVariables((resultTypeTerm, "field"), ("boolean", "isNull"))
        val code = s"""
                      |$nullTerm = true;
                      |$fieldTerm = $defaultValue;
                      |if ($buildRow != null) {
                      |  ${expr.getCode}
                      |  $nullTerm = ${expr.nullTerm};
                      |  $fieldTerm = ${expr.resultTerm};
                      |}
          """.stripMargin
        GeneratedExpression(fieldTerm, nullTerm, code, fieldType)
    }
  }

  /** Generates the input row variables expr. */
  def genInputVars(inputRowTerm: String, inputType: RowType): Seq[GeneratedExpression] = {
    val indices = fieldIndices(inputType)
    val buildExprs = indices
      .map(
        index => GenerateUtils.generateFieldAccess(getOperatorCtx, inputType, inputRowTerm, index))
      .toSeq
    indices.foreach(
      index =>
        getOperatorCtx
          .addReusableInputUnboxingExprs(inputRowTerm, index, buildExprs(index)))

    buildExprs
  }

  override def usedInputVars(inputId: Int): util.Set[Integer] = {
    if (inputId == buildInputId) {
      val set: util.Set[Integer] = new util.HashSet[Integer]()
      buildKeys.toStream.map(key => set.add(key))
      set
    } else {
      super.usedInputVars(inputId)
    }
  }

  override def getInputRowDataClass(inputId: Int): Class[_ <: RowData] = {
    if (inputId == buildInputId) {
      // To build side, we wrap it BinaryRowData
      classOf[BinaryRowData]
    } else {
      super.getInputRowDataClass(inputId)
    }
  }

  private def codegenHashTable(spillEnabled: Boolean): Unit = {
    val buildSer = new BinaryRowDataSerializer(buildType.getFieldCount)
    val buildSerTerm = operatorCtx.addReusableObject(buildSer, "buildSer")
    val probeSer = new BinaryRowDataSerializer(probeType.getFieldCount)
    val probeSerTerm = operatorCtx.addReusableObject(probeSer, "probeSer")

    val bGenProj =
      genProjection(
        operatorCtx.tableConfig,
        operatorCtx.classLoader,
        buildType.getChildren.toArray(Array[LogicalType]()))
    operatorCtx.addReusableInnerClass(bGenProj.getClassName, bGenProj.getCode)
    val pGenProj =
      genProjection(
        operatorCtx.tableConfig,
        operatorCtx.classLoader,
        probeType.getChildren.toArray(Array[LogicalType]()))
    operatorCtx.addReusableInnerClass(pGenProj.getClassName, pGenProj.getCode)

    operatorCtx.addReusableMember(s"${bGenProj.getClassName} $buildToBinaryRow;")
    val buildProjRefs = operatorCtx.addReusableObject(bGenProj.getReferences, "buildProjRefs")
    operatorCtx.addReusableInitStatement(
      s"$buildToBinaryRow = new ${bGenProj.getClassName}($buildProjRefs);")

    operatorCtx.addReusableMember(s"${pGenProj.getClassName} $probeToBinaryRow;")
    val probeProjRefs = operatorCtx.addReusableObject(pGenProj.getReferences, "probeProjRefs")
    operatorCtx.addReusableInitStatement(
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
         |      ${buildRowCount}L / getRuntimeContext().getNumberOfParallelSubtasks(),
         |      $spillEnabled);
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
    operatorCtx.addReusableInnerClass(hashTableClassTerm, tableCode)
    operatorCtx.addReusableMember(s"$hashTableClassTerm $hashTableTerm;")
    val memorySizeTerm = newName("memorySize")
    operatorCtx.addReusableOpenStatement(
      s"long $memorySizeTerm = computeMemorySize(${fusionContext.getManagedMemoryFraction});")
    operatorCtx.addReusableOpenStatement(
      s"$hashTableTerm = new $hashTableClassTerm($memorySizeTerm);")

    operatorCtx.addReusableCloseStatement(s"""
                                             |if (this.$hashTableTerm != null) {
                                             |  this.$hashTableTerm.close();
                                             |  this.$hashTableTerm.free();
                                             |  this.$hashTableTerm = null;
                                             |}
       """.stripMargin)
  }
}
