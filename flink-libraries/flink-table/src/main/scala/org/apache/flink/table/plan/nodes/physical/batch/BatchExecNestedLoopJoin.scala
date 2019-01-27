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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.types.{DataTypes, RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.{FIRST, SECOND, generatorCollect}
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.runtime.TwoInputSubstituteStreamOperator
import org.apache.flink.table.runtime.util.ResettableExternalBuffer
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

trait BatchExecNestedLoopJoinBase extends BatchExecJoinBase {

  val leftIsBuild: Boolean
  val singleRowJoin: Boolean

  val input1Term: String = DEFAULT_INPUT1_TERM
  val input2Term: String = DEFAULT_INPUT2_TERM
  val (buildRow, buildArity, probeRow, probeArity, probeSelection) = {
    val leftArity = getLeft.getRowType.getFieldCount
    val rightArity = getRight.getRowType.getFieldCount
    if (leftIsBuild) {
      (input1Term, leftArity, input2Term, rightArity, SECOND)
    } else {
      (input2Term, rightArity, input1Term, leftArity, FIRST)
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("build", if (leftIsBuild) "left" else "right")
      .itemIf("singleRowJoin", singleRowJoin, singleRowJoin)
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    // Assume NestedLoopJoin always broadcast data from child which smaller.
    pushDownTraitsIntoBroadcastJoin(requiredTraitSet, leftIsBuild)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }
    val buildRel= if (leftIsBuild) getLeft else getRight
    val memoryCost = mq.getRowCount(buildRel) *
          (mq.getAverageRowSize(buildRel) + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES) *
        shuffleBuildCount(mq)
    val cpuCost = leftRowCnt * rightRowCnt
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memoryCost)
  }

  private[flink] def shuffleBuildCount(mq: RelMetadataQuery): Int = {
    val probeRel = if (leftIsBuild) getRight else getLeft
    val rowCount = mq.getRowCount(probeRel)
    if (rowCount == null) {
      1
    } else {
      Math.max(1,
        (rowCount * mq.getAverageRowSize(probeRel) /
          SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE).toInt)
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig

    val leftInput = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightInput = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val leftType = TypeConverters.createInternalTypeFromTypeInfo(
      leftInput.getOutputType).asInstanceOf[RowType]
    val rightType = TypeConverters.createInternalTypeFromTypeInfo(
      rightInput.getOutputType).asInstanceOf[RowType]

    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, flinkJoinType.isOuter, config.getNullCheck)
        .bindInput(leftType, inputTerm = input1Term)
        .bindSecondInput(rightType, inputTerm = input2Term)

    // otherwise we use ResettableExternalBuffer to prevent OOM
    val buffer = newName("resettableExternalBuffer")
    val iter = newName("iter")

    // input row might not be binary row, need a serializer
    val isFirstRow = newName("isFirstRow")
    val isBinaryRow = newName("isBinaryRow")

    val externalBufferMemorySize = getResource.getReservedManagedMem * NodeResourceUtil.SIZE_IN_MB

    if (singleRowJoin) {
      ctx.addReusableMember(s"$BASE_ROW $buildRow = null;")
    } else {
      ctx.addReusableMember(s"boolean $isFirstRow = true;")
      ctx.addReusableMember(s"boolean $isBinaryRow = false;")

      val serializer = newName("serializer")
      def initSerializer(i: Int): Unit = {
        ctx.addReusableOpenStatement(
          s"""
             |$ABSTRACT_ROW_SERIALIZER $serializer =
             |  ($ABSTRACT_ROW_SERIALIZER) getOperatorConfig()
             |    .getTypeSerializerIn$i(getUserCodeClassloader());
             |""".stripMargin)
      }
      if (leftIsBuild) initSerializer(1) else initSerializer(2)

      ctx.addReusableResettableExternalBuffer(buffer, externalBufferMemorySize, serializer)
      ctx.addReusableCloseStatement(s"$buffer.close();")

      val iterTerm = classOf[ResettableExternalBuffer#BufferIterator].getCanonicalName
      ctx.addReusableMember(s"$iterTerm $iter = null;")
    }

    val condExpr = exprGenerator.generateExpression(getCondition)

    val buildRowSer = ctx.addReusableTypeSerializer(if (leftIsBuild) leftType else rightType)

    val buildProcessCode = if (singleRowJoin) {
      s"this.$buildRow = ($BASE_ROW) $buildRowSer.copy($buildRow);"
    } else {
      s"$buffer.add(($BASE_ROW) $buildRow);"
    }

    val (probeProcessCode, buildEndCode, probeEndCode) =
      genProcessAndEndCode(ctx, condExpr, iter, buffer)

    // build first or second
    val (firstInputCode, processCode1, endInputCode1, processCode2, endInputCode2)  =
      if (leftIsBuild) {
        (
            FIRST,
            s"""
               |$buildProcessCode
               |return $FIRST;
             """.stripMargin,
            s"""
               |$buildEndCode
             """.stripMargin,
            probeProcessCode,
            s"""
               |$probeEndCode
             """.stripMargin
            )
      }  else {
        (
            SECOND,
            probeProcessCode,
            s"""
               |$probeEndCode
             """.stripMargin,
            s"""
               |$buildProcessCode
               |return $SECOND;
             """.stripMargin,
            s"""
               |$buildEndCode
             """.stripMargin
            )
      }

    // generator operatorExpression
    val operatorExpression =
      OperatorCodeGenerator.generateTwoInputStreamOperator[BaseRow, BaseRow, BaseRow](
        ctx,
        description,
        s"return $firstInputCode;",
        processCode1,
        endInputCode1,
        processCode2,
        endInputCode2,
        leftType,
        rightType,
        input1Term = input1Term,
        input2Term = input2Term
      )

    val substituteStreamOperator = new TwoInputSubstituteStreamOperator[BaseRow, BaseRow, BaseRow](
      operatorExpression.name,
      operatorExpression.code)

    val transformation = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftInput,
      rightInput,
      getOperatorName,
      substituteStreamOperator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      getResource.getParallelism
    )
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setReadOrderHint(
      if (leftIsBuild) ReadOrder.INPUT1_FIRST else ReadOrder.INPUT2_FIRST)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName: String = {
    val joinExpressionStr = if (getCondition != null) {
      val inFields = inputDataType.getFieldNames.toList
      s"where: ${getExpressionString(getCondition, inFields, None, ExpressionFormat.Infix)}, "
    } else {
      ""
    }
    s"NestedLoopJoin($joinExpressionStr${if (leftIsBuild) "buildLeft" else "buildRight"})"
  }

  def newIter(iter: String, buffer: String): String = {
    s"""
       |if ($iter == null) {
       |  $iter = $buffer.newIterator();
       |} else {
       |  $iter.reset();
       |}
       |""".stripMargin
  }

  /**
    * @return (processCode, buildEndCode, probeEndCode).
    */
  def genProcessAndEndCode(
      ctx: CodeGeneratorContext,
      condExpr: GeneratedExpression,
      iter: String,
      buffer: String): (String, String, String)
}

class BatchExecNestedLoopJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    val leftIsBuild: Boolean,
    joinCondition: RexNode,
    joinType: JoinRelType,
    val singleRowJoin: Boolean,
    val description: String)
  extends Join(cluster, traitSet, left, right, joinCondition, Set.empty[CorrelationId], joinType)
  with BatchExecNestedLoopJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join =
    new BatchExecNestedLoopJoin(
      cluster,
      traitSet,
      left,
      right,
      leftIsBuild,
      conditionExpr,
      joinType,
      singleRowJoin,
      description)

  override def genProcessAndEndCode(
      ctx: CodeGeneratorContext,
      condExpr: GeneratedExpression,
      iter: String,
      buffer: String): (String, String, String) = {
    val joinedRow = newName("joinedRow")
    val buildRowMatched: String = newName("buildRowMatched")
    val buildNullRow = newName("buildNullRow")
    val probeNullRow = newName("probeNullRow")

    val isFull: Boolean = flinkJoinType == FlinkJoinRelType.FULL
    val probeOuter = flinkJoinType.isOuter

    ctx.addOutputRecord(
      FlinkTypeFactory.toInternalRowType(getRowType), classOf[JoinedRow], joinedRow)
    ctx.addReusableNullRow(buildNullRow, buildArity)

    val bitSetTerm = classOf[util.BitSet].getCanonicalName
    if (isFull) {
      ctx.addReusableNullRow(probeNullRow, probeArity)
      if (singleRowJoin) {
        ctx.addReusableMember(s"boolean $buildRowMatched = false;")
      } else {
        // BitSet is slower than boolean[].
        // We can use boolean[] when there are a small number of records.
        ctx.addReusableMember(s"$bitSetTerm $buildRowMatched = null;")
      }
    }

    val probeOuterCode =
      s"""
         |if (!matched) {
         |  ${generatorCollect(
                if (leftIsBuild) {
                  s"$joinedRow.replace($buildNullRow, $probeRow)"
                } else {
                  s"$joinedRow.replace($probeRow, $buildNullRow)"
                })}
         |}
            """.stripMargin

    val goJoin = {
      s"""
         |${ctx.reusePerRecordCode()}
         |${ctx.reuseInputUnboxingCode(Set(buildRow))}
         |${condExpr.code}
         |if (${condExpr.resultTerm}) {
         |  ${generatorCollect(s"$joinedRow.replace($input1Term, $input2Term)")}
         |  ${if (probeOuter) "matched = true;" else ""}
         |""".stripMargin
    }

    val checkMatched = if (singleRowJoin) {
      s"""
         |if ($buildRow != null) {
         |  $goJoin
         |  ${if (isFull) s"$buildRowMatched = true;" else ""}
         |  }
         |}
       """.stripMargin
    } else {
      s"""
         |${newIter(iter, buffer)}
         |${if (isFull) s"int iterCnt = -1;" else ""}
         |while ($iter.advanceNext()) {
         |  ${if (isFull) s"iterCnt++;" else ""}
         |  $BINARY_ROW $buildRow = $iter.getRow();
         |  $goJoin
         |  ${if (isFull) s"$buildRowMatched.set(iterCnt);" else ""}
         |  }
         |}
         |""".stripMargin
    }

    val processCode =
      s"""
         |${if (probeOuter) "boolean matched = false;" else ""}
         |${ctx.reuseInputUnboxingCode(Set(probeRow))}
         |$checkMatched
         |${if (probeOuter) probeOuterCode else ""}
         |return $probeSelection;
         |""".stripMargin

    var buildEndCode = if (!singleRowJoin && isFull) {
      s"$buildRowMatched = new $bitSetTerm($buffer.size());"
    } else {
      ""
    }

    buildEndCode =
        s"""
           |LOG.info("Finish build phase.");
           |$buildEndCode
       """.stripMargin

    val buildOuterEmit = generatorCollect(
      if (leftIsBuild) {
        s"$joinedRow.replace($buildRow, $probeNullRow)"
      } else {
        s"$joinedRow.replace($probeNullRow, $buildRow)"
      })

    var probeEndCode = if (isFull) {
      if (singleRowJoin) {
        s"""
           |if ($buildRow != null && !$buildRowMatched) {
           |  $buildOuterEmit
           |}
         """.stripMargin
      } else {
        s"""
           |${newIter(iter, buffer)}
           |int iterCnt = -1;
           |while ($iter.advanceNext()) {
           |  iterCnt++;
           |  $BINARY_ROW $buildRow = $iter.getRow();
           |  if (!$buildRowMatched.get(iterCnt)) {
           |    $buildOuterEmit
           |  }
           |}
           |""".stripMargin
      }
    } else {
      ""
    }

    probeEndCode =
      s"""
         |LOG.info("Finish probe phase.");
         |$probeEndCode
         |LOG.info("Finish rebuild phase.");
       """.stripMargin

    (processCode, buildEndCode, probeEndCode)
  }
}

class BatchExecNestedLoopSemiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    joinCondition: RexNode,
    leftKeys: ImmutableIntList,
    rightKeys: ImmutableIntList,
    isAntiJoin: Boolean,
    val singleRowJoin: Boolean,
    val description: String)
  extends SemiJoin(cluster, traitSet, left, right, joinCondition, leftKeys, rightKeys, isAntiJoin)
  with BatchExecNestedLoopJoinBase {

  val leftIsBuild: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      condition: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): SemiJoin = {
    val joinInfo = JoinInfo.of(left, right, condition)
    new BatchExecNestedLoopSemiJoin(
      cluster,
      traitSet,
      left,
      right,
      condition,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      isAnti,
      singleRowJoin,
      description)
  }

  override def genProcessAndEndCode(
      ctx: CodeGeneratorContext,
      condExpr: GeneratedExpression,
      iter: String,
      buffer: String): (String, String, String) = {
    val checkMatchedCode = if (singleRowJoin) {
      s"""
         |if ($buildRow != null) {
         |  ${ctx.reusePerRecordCode()}
         |  ${ctx.reuseInputUnboxingCode(Set(buildRow))}
         |  ${condExpr.code}
         |  if (${condExpr.resultTerm}) {
         |    matched = true;
         |  }
         |}
         |""".stripMargin
    } else {
      s"""
         |${newIter(iter, buffer)}
         |while ($iter.advanceNext()) {
         |  $BINARY_ROW $buildRow = $iter.getRow();
         |  ${ctx.reusePerRecordCode()}
         |  ${ctx.reuseInputUnboxingCode(Set(buildRow))}
         |  ${condExpr.code}
         |  if (${condExpr.resultTerm}) {
         |    matched = true;
         |    break;
         |  }
         |}
         |""".stripMargin
    }

    (s"""
       |boolean matched = false;
       |${ctx.reuseInputUnboxingCode(Set(probeRow))}
       |$checkMatchedCode
       |if (${if (isAnti) "!" else ""}matched) {
       |  ${generatorCollect(probeRow)}
       |}
       |return $probeSelection;
       |""".stripMargin, "", "")
  }
}
