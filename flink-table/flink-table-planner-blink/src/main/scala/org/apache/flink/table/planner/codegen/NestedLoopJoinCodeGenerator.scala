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

import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.{INPUT_SELECTION, generateCollect}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.typeutils.AbstractRowSerializer
import org.apache.flink.table.runtime.util.{LazyMemorySegmentPool, ResettableExternalBuffer}
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex.RexNode

import java.util

/**
  * Code gen for nested loop join.
  */
class NestedLoopJoinCodeGenerator(
    ctx: CodeGeneratorContext,
    singleRowJoin: Boolean,
    leftIsBuild: Boolean,
    leftType: RowType,
    rightType: RowType,
    outputType: RowType,
    joinType: FlinkJoinType,
    condition: RexNode) {

  val (buildRow, buildArity, probeRow, probeArity) = {
    val leftArity = leftType.getFieldCount
    val rightArity = rightType.getFieldCount
    if (leftIsBuild) {
      (DEFAULT_INPUT1_TERM, leftArity, DEFAULT_INPUT2_TERM, rightArity)
    } else {
      (DEFAULT_INPUT2_TERM, rightArity, DEFAULT_INPUT1_TERM, leftArity)
    }
  }

  def gen(): CodeGenOperatorFactory[BaseRow] = {
    val exprGenerator = new ExprCodeGenerator(ctx, joinType.isOuter)
        .bindInput(leftType).bindSecondInput(rightType)

    // we use ResettableExternalBuffer to prevent OOM
    val buffer = newName("resettableExternalBuffer")
    val iter = newName("iter")

    // input row might not be binary row, need a serializer
    val isFirstRow = newName("isFirstRow")
    val isBinaryRow = newName("isBinaryRow")

    if (singleRowJoin) {
      ctx.addReusableMember(s"$BASE_ROW $buildRow = null;")
    } else {
      ctx.addReusableMember(s"boolean $isFirstRow = true;")
      ctx.addReusableMember(s"boolean $isBinaryRow = false;")

      val serializer = newName("serializer")
      def initSerializer(i: Int): Unit = {
        ctx.addReusableOpenStatement(
          s"""
             |${className[AbstractRowSerializer[_]]} $serializer =
             |  (${className[AbstractRowSerializer[_]]}) getOperatorConfig()
             |    .getTypeSerializerIn$i(getUserCodeClassloader());
             |""".stripMargin)
      }
      if (leftIsBuild) initSerializer(1) else initSerializer(2)

      addReusableResettableExternalBuffer(buffer, serializer)
      ctx.addReusableCloseStatement(s"$buffer.close();")

      val iterTerm = classOf[ResettableExternalBuffer#BufferIterator].getCanonicalName
      ctx.addReusableMember(s"$iterTerm $iter = null;")
    }

    val condExpr = exprGenerator.generateExpression(condition)

    val buildRowSer = ctx.addReusableTypeSerializer(if (leftIsBuild) leftType else rightType)

    val buildProcessCode = if (singleRowJoin) {
      s"this.$buildRow = ($BASE_ROW) $buildRowSer.copy($buildRow);"
    } else {
      s"$buffer.add(($BASE_ROW) $buildRow);"
    }

    var (probeProcessCode, buildEndCode, probeEndCode) =
      if (joinType == FlinkJoinType.SEMI || joinType == FlinkJoinType.ANTI) {
        genSemiJoinProcessAndEndCode(condExpr, iter, buffer)
      } else {
        genJoinProcessAndEndCode(condExpr, iter, buffer)
      }

    val buildEnd = newName("buildEnd")
    ctx.addReusableMember(s"private transient boolean $buildEnd = false;")
    buildEndCode =
        (if (singleRowJoin) buildEndCode else s"$buffer.complete(); \n $buildEndCode") +
            s"\n $buildEnd = true;"

    // build first or second
    val (processCode1, endInputCode1, processCode2, endInputCode2)  =
      if (leftIsBuild) {
        (buildProcessCode, buildEndCode, probeProcessCode, probeEndCode)
      } else {
        (probeProcessCode, probeEndCode, buildProcessCode, buildEndCode)
      }

    // generator operatorExpression
    val genOp = OperatorCodeGenerator.generateTwoInputStreamOperator[BaseRow, BaseRow, BaseRow](
      ctx,
      "BatchNestedLoopJoin",
      processCode1,
      processCode2,
      leftType,
      rightType,
      nextSelectionCode = Some(
        s"""
           |if ($buildEnd) {
           |  return $INPUT_SELECTION.${if (leftIsBuild) "SECOND" else "FIRST"};
           |} else {
           |  return $INPUT_SELECTION.${if (leftIsBuild) "FIRST" else "SECOND"};
           |}
         """.stripMargin),
      endInputCode1 = Some(endInputCode1),
      endInputCode2 = Some(endInputCode2))
    new CodeGenOperatorFactory[BaseRow](genOp)
  }

  /**
    * Deal with inner join, left outer join, right outer join and full outer join.
    */
  private def genJoinProcessAndEndCode(
      condExpr: GeneratedExpression, iter: String, buffer: String): (String, String, String) = {
    val joinedRowTerm = newName("joinedRow")
    def joinedRow(row1: String, row2: String): String = {
      s"$joinedRowTerm.replace($row1, $row2)"
    }

    val buildMatched = newName("buildMatched")
    val probeMatched = newName("probeMatched")
    val buildNullRow = newName("buildNullRow")
    val probeNullRow = newName("probeNullRow")

    val isFull = joinType == FlinkJoinType.FULL
    val probeOuter = joinType.isOuter

    ctx.addReusableOutputRecord(outputType, classOf[JoinedRow], joinedRowTerm)
    ctx.addReusableNullRow(buildNullRow, buildArity)

    val bitSetTerm = classOf[util.BitSet].getCanonicalName
    if (isFull) {
      ctx.addReusableNullRow(probeNullRow, probeArity)
      if (singleRowJoin) {
        ctx.addReusableMember(s"boolean $buildMatched = false;")
      } else {
        // BitSet is slower than boolean[].
        // We can use boolean[] when there are a small number of records.
        ctx.addReusableMember(s"$bitSetTerm $buildMatched = null;")
      }
    }
    val collectorRow = if (leftIsBuild) {
      joinedRow(buildNullRow, probeRow)
    } else {
      joinedRow(probeRow, buildNullRow)
    }

    val probeOuterCode =
      s"""
         |if (!$probeMatched) {
         |  ${generateCollect(collectorRow)}
         |}
       """.stripMargin

    val iterCnt = newName("iteratorCount")
    val joinBuildAndProbe = {
      s"""
         |${ctx.reusePerRecordCode()}
         |${ctx.reuseInputUnboxingCode(buildRow)}
         |${condExpr.code}
         |if (${condExpr.resultTerm}) {
         |  ${generateCollect(joinedRow(DEFAULT_INPUT1_TERM, DEFAULT_INPUT2_TERM))}
         |
         |  // set probe outer matched flag
         |  ${if (probeOuter) s"$probeMatched = true;" else ""}
         |
         |  // set build outer matched flag
         |  ${if (singleRowJoin) {
                if (isFull) s"$buildMatched = true;" else ""
              } else {
                if (isFull) s"$buildMatched.set($iterCnt);" else ""
              }
            }
         |}
         |""".stripMargin
    }

    val goJoin = if (singleRowJoin) {
      s"""
         |if ($buildRow != null) {
         |  $joinBuildAndProbe
         |}
       """.stripMargin
    } else {
      s"""
         |${resetIterator(iter, buffer)}
         |${if (isFull) s"int $iterCnt = -1;" else ""}
         |while ($iter.advanceNext()) {
         |  ${if (isFull) s"$iterCnt++;" else ""}
         |  $BINARY_ROW $buildRow = $iter.getRow();
         |  $joinBuildAndProbe
         |}
         |""".stripMargin
    }

    val processCode =
      s"""
         |${if (probeOuter) s"boolean $probeMatched = false;" else ""}
         |${ctx.reuseInputUnboxingCode(probeRow)}
         |$goJoin
         |${if (probeOuter) probeOuterCode else ""}
         |""".stripMargin

    val buildEndCode =
        s"""
           |LOG.info("Finish build phase.");
           |${
              if (!singleRowJoin && isFull) {
                s"$buildMatched = new $bitSetTerm($buffer.size());"
              } else {
                ""
              }
             }
           |""".stripMargin

    val buildOuterEmit = generateCollect(
      if (leftIsBuild) joinedRow(buildRow, probeNullRow) else joinedRow(probeNullRow, buildRow))

    var probeEndCode = if (isFull) {
      if (singleRowJoin) {
        s"""
           |if ($buildRow != null && !$buildMatched) {
           |  $buildOuterEmit
           |}
         """.stripMargin
      } else {
        val iterCnt = newName("iteratorCount")
        s"""
           |${resetIterator(iter, buffer)}
           |int $iterCnt = -1;
           |while ($iter.advanceNext()) {
           |  $iterCnt++;
           |  $BINARY_ROW $buildRow = $iter.getRow();
           |  if (!$buildMatched.get($iterCnt)) {
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
           |$probeEndCode
           |LOG.info("Finish probe phase.");
       """.stripMargin

    (processCode, buildEndCode, probeEndCode)
  }

  /**
    * Deal with semi join and anti join.
    */
  private def genSemiJoinProcessAndEndCode(
      condExpr: GeneratedExpression, iter: String, buffer: String): (String, String, String) = {
    val probeMatched = newName("probeMatched")
    val goJoin = if (singleRowJoin) {
      s"""
         |if ($buildRow != null) {
         |  ${ctx.reusePerRecordCode()}
         |  ${ctx.reuseInputUnboxingCode(buildRow)}
         |  ${condExpr.code}
         |  if (${condExpr.resultTerm}) {
         |    $probeMatched = true;
         |  }
         |}
         |""".stripMargin
    } else {
      s"""
         |${resetIterator(iter, buffer)}
         |while ($iter.advanceNext()) {
         |  $BINARY_ROW $buildRow = $iter.getRow();
         |  ${ctx.reusePerRecordCode()}
         |  ${ctx.reuseInputUnboxingCode(buildRow)}
         |  ${condExpr.code}
         |  if (${condExpr.resultTerm}) {
         |    $probeMatched = true;
         |    break;
         |  }
         |}
         |""".stripMargin
    }

    (s"""
        |boolean $probeMatched = false;
        |${ctx.reuseInputUnboxingCode(probeRow)}
        |$goJoin
        |if (${if (joinType == FlinkJoinType.ANTI) "!" else ""}$probeMatched) {
        |  ${generateCollect(probeRow)}
        |}
        |""".stripMargin, "", "")
  }

  /**
    * Reset or new a iterator.
    */
  def resetIterator(iter: String, buffer: String): String = {
    s"""
       |if ($iter == null) {
       |  $iter = $buffer.newIterator();
       |} else {
       |  $iter.reset();
       |}
       |""".stripMargin
  }

  private def addReusableResettableExternalBuffer(
      fieldTerm: String, serializer: String): Unit = {
    val memManager = "getContainingTask().getEnvironment().getMemoryManager()"
    val ioManager = "getContainingTask().getEnvironment().getIOManager()"

    val open =
      s"""
         |$fieldTerm = new ${className[ResettableExternalBuffer]}(
         |  $ioManager,
         |  new ${className[LazyMemorySegmentPool]}(
         |    getContainingTask(),
         |    $memManager,
         |    (int) (computeMemorySize() / $memManager.getPageSize())),
         |  $serializer,
         |  false);
         |""".stripMargin
    ctx.addReusableMember(s"${className[ResettableExternalBuffer]} $fieldTerm = null;")
    ctx.addReusableOpenStatement(open)
  }
}


