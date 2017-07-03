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
package org.apache.flink.table.runtime.join

import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenerator, ExpressionReducer}
import org.apache.flink.table.functions.TimeMaterializationSqlFunction
import org.apache.flink.table.plan.schema.{RowSchema, TimeIndicatorRelDataType}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * An util class to help analyze and build join code .
  */
object WindowJoinUtil {

  /**
    * Analyze time-condtion to get time boundary for each stream and get the time type
    * and return remain condition.
    *
    * @param  condition   join condition
    * @param  leftLogicalFieldCnt left stream logical field num
    * @param  leftPhysicalFieldCnt left stream physical field num
    * @param  inputType   left and right connect stream type
    * @param  rexBuilder   util to build rexNode
    * @param  config      table environment config
    * @return  timetype, left lower boundary, right lower boundary, remain condition
    */
  private[flink] def analyzeTimeBoundary(
      condition: RexNode,
      leftLogicalFieldCnt: Int,
      leftPhysicalFieldCnt: Int,
      inputType: RelDataType,
      rexBuilder: RexBuilder,
      config: TableConfig): (RelDataType, Long, Long, Option[RexNode]) = {

    // Converts the condition to conjunctive normal form (CNF)
    val cnfCondition = RexUtil.toCnf(rexBuilder, condition)

    // split the condition into time indicator condition and other condition
    val (timeTerms, remainTerms) =
      splitJoinCondition(
        cnfCondition,
        leftLogicalFieldCnt,
        inputType
      )

    if (timeTerms.size != 2) {
      throw new TableException("A time-based stream join requires exactly " +
        "two join predicates that bound the time in both directions.")
    }

    // extract time offset from the time indicator conditon
    val streamTimeOffsets =
      timeTerms.map(x => extractTimeOffsetFromCondition(x._3, x._2, rexBuilder, config))

    val (leftLowerBound, leftUpperBound) =
      streamTimeOffsets match {
        case Seq((x, true), (y, false)) => (x, y)
        case Seq((x, false), (y, true)) => (y, x)
        case _ =>
          throw new TableException(
            "Time-based join conditions must reference the time attribute of both input tables.")
      }

    // compose the remain condition list into one condition
    val remainCondition =
      remainTerms match {
        case Seq() => None
        case _ =>
          // turn the logical field index to physical field index
          def transInputRef(expr: RexNode): RexNode = {
            expr match {
              case c: RexCall =>
                val newOps = c.operands.map(transInputRef(_))
                rexBuilder.makeCall(c.getType, c.getOperator, newOps)
              case i: RexInputRef if i.getIndex >= leftLogicalFieldCnt =>
                rexBuilder.makeInputRef(
                  i.getType,
                  i.getIndex - leftLogicalFieldCnt + leftPhysicalFieldCnt)
              case _ => expr
            }
          }

          Some(remainTerms.map(transInputRef(_)).reduceLeft( (l, r) => {
            RelOptUtil.andJoinFilters(rexBuilder, l, r)
          }))
      }

    (timeTerms.get(0)._1, leftLowerBound, leftUpperBound, remainCondition)
  }

  /**
   * Split the join conditions into time condition and non-time condition
   *
   * @return (Seq(timeTerms), Seq(remainTerms)),
   */
  private def splitJoinCondition(
      cnfCondition: RexNode,
      leftFieldCount: Int,
      inputType: RelDataType): (Seq[(RelDataType, Boolean, RexNode)], Seq[RexNode]) = {

    cnfCondition match {
      case c: RexCall if c.getKind == SqlKind.AND =>
        val timeIndicators =
          c.operands.map(splitJoinCondition(_, leftFieldCount, inputType))
        timeIndicators.reduceLeft { (l, r) =>
          (l._1 ++ r._1, l._2 ++ r._2)
        }
      case c: RexCall =>
        val timeIndicators = extractTimeIndicatorAccesses(c, leftFieldCount, inputType)
        timeIndicators match {
          case Seq() =>
            (Seq(), Seq(c))
          case Seq(v1, v2) =>
            if (v1._1 != v2._1) {
              throw new TableException(
                "Both time attributes in a join condition must be of the same type.")
            }
            if (v1._2 == v2._2) {
              throw new TableException("Time-based join conditions " +
                "must reference the time attribute of both input tables.")
            }
            (Seq((v1._1, v1._2, c)), Seq())
          case _ =>
            throw new TableException(
              "Time-based join conditions must reference the time attribute of both input tables.")
        }
      case other =>
        val timeIndicators = extractTimeIndicatorAccesses(other, leftFieldCount, inputType)
        timeIndicators match {
          case Seq() =>
            (Seq(), Seq(other))
          case _ =>
            throw new TableException("Time indicators can not be used in non time-condition.")
        }
    }
  }

  /**
   * analysis if condition term has time indicator
    *
   * @return seq(timeType, is left input time indicator)
   */
  def extractTimeIndicatorAccesses(
      expression: RexNode,
      leftFieldCount: Int,
      inputType: RelDataType): Seq[(RelDataType, Boolean)] = {

    expression match {
      case i: RexInputRef =>
        val idx = i.getIndex
        inputType.getFieldList.get(idx).getType match {
          case t: TimeIndicatorRelDataType if idx < leftFieldCount =>
            // left table time indicator
            Seq((t, true))
          case t: TimeIndicatorRelDataType =>
            // right table time indicator
            Seq((t, false))
          case _ => Seq()
        }
      case c: RexCall =>
        c.operands.map(extractTimeIndicatorAccesses(_, leftFieldCount, inputType)).reduce(_++_)
      case _ => Seq()
    }
  }

  /**
    * Extract time offset and determain it's the lower bound of left stream or the upper bound
    *
    * @return window boundary, is left lower bound
    */
  def extractTimeOffsetFromCondition(
      timeTerm: RexNode,
      isLeftExprBelongLeftTable: Boolean,
      rexBuilder: RexBuilder,
      config: TableConfig): (Long, Boolean) = {

    val timeCall: RexCall = timeTerm.asInstanceOf[RexCall]

    val isLeftLowerBound: Boolean =
      timeTerm.getKind match {
        // e.g a.proctime > b.proctime - 5 sec, then it's the lower bound of a and the value is -5
        // e.g b.proctime > a.proctime - 5 sec, then it's not the lower bound of a but upper bound
        case kind @ (SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL) =>
          isLeftExprBelongLeftTable
        // e.g a.proctime < b.proctime + 5 sec, the the upper bound of a is 5
        case kind @ (SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL) =>
          !isLeftExprBelongLeftTable
        case _ =>
          throw new TableException("Unsupport time-condition.")
      }

    val (leftLiteral, rightLiteral) =
      reduceTimeExpression(
        timeCall.operands.get(0),
        timeCall.operands.get(1),
        rexBuilder,
        config)
    val tmpTimeOffset: Long =
      if (isLeftExprBelongLeftTable) rightLiteral - leftLiteral else leftLiteral - rightLiteral

    val boundary =
      tmpTimeOffset.signum * (
        if (timeTerm.getKind == SqlKind.LESS_THAN || timeTerm.getKind == SqlKind.GREATER_THAN) {
          tmpTimeOffset.abs - 1
        } else {
          tmpTimeOffset.abs
        })

    (boundary, isLeftLowerBound)
  }

  /**
    * Calcute the time boundary. Replace the rowtime/proctime with zero literal.
    * For example:
    * b.proctime - interval '1' second - interval '2' second will be translate to
    * 0 - 1000 - 2000
    */
  private def reduceTimeExpression(
      leftRexNode: RexNode,
      rightRexNode: RexNode,
      rexBuilder: RexBuilder,
      config: TableConfig): (Long, Long) = {

    /**
      * replace the rowtime/proctime with zero literal.
      */
    def replaceTimeFieldWithLiteral(expr: RexNode): RexNode = {
      expr match {
        case c: RexCall if c.getOperator == TimeMaterializationSqlFunction =>
          // replace with timestamp
          rexBuilder.makeZeroLiteral(expr.getType)
        case c: RexCall =>
          // replace in call operands
          val newOps = c.operands.map(replaceTimeFieldWithLiteral(_))
          rexBuilder.makeCall(c.getType, c.getOperator, newOps)
        case i: RexInputRef if FlinkTypeFactory.isTimeIndicatorType(i.getType) =>
          // replace with timestamp
          rexBuilder.makeZeroLiteral(expr.getType)
        case _: RexInputRef =>
          throw new TableException("Time join condition may only reference time indicator fields.")
        case _ => expr
      }
    }

    val literalLeftRex = replaceTimeFieldWithLiteral(leftRexNode)
    val literalRightRex = replaceTimeFieldWithLiteral(rightRexNode)

    val exprReducer = new ExpressionReducer(config)
    val originList = new util.ArrayList[RexNode]()
    originList.add(literalLeftRex)
    originList.add(literalRightRex)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    val literals = reduceList.map(f => f match {
      case call: RexCall =>
        call.getOperands.get(0).asInstanceOf[RexLiteral].getValue2.asInstanceOf[Long]
      case literal: RexLiteral =>
        literal.getValue2.asInstanceOf[Long]
      case _ =>
        throw TableException(
          "Time condition may only consist of time attributes, literals, and arithmetic operators.")
    })

    (literals.get(0), literals.get(1))
  }


  /**
    * Generate other non-equi condition function
    * @param  config   table env config
    * @param  joinType  join type to determain whether input can be null
    * @param  leftType  left stream type
    * @param  rightType  right stream type
    * @param  returnType   return type
    * @param  otherCondition   non-equi condition
    * @param  ruleDescription  rule description
    */
  private[flink] def generateJoinFunction(
      config: TableConfig,
      joinType: JoinRelType,
      leftType: TypeInformation[Row],
      rightType: TypeInformation[Row],
      returnType: RowSchema,
      otherCondition: Option[RexNode],
      ruleDescription: String) = {

    // whether input can be null
    val nullCheck = joinType match {
      case JoinRelType.INNER => false
      case JoinRelType.LEFT  => true
      case JoinRelType.RIGHT => true
      case JoinRelType.FULL  => true
    }

    // generate other non-equi function code
    val generator = new CodeGenerator(
      config,
      nullCheck,
      leftType,
      Some(rightType))

    val conversion = generator.generateConverterResultExpression(
      returnType.physicalTypeInfo,
      returnType.physicalType.getFieldNames)

    // if other condition is none, then output the result directly
    val body = otherCondition match {
      case None =>
        s"""
           |${conversion.code}
           |${generator.collectorTerm}.collect(${conversion.resultTerm});
           |""".stripMargin
      case Some(remainCondition) =>
        val genCond = generator.generateExpression(remainCondition)
        s"""
           |${genCond.code}
           |if (${genCond.resultTerm}) {
           |  ${conversion.code}
           |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
           |}
           |""".stripMargin
    }

    generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Row, Row, Row]],
      body,
      returnType.physicalTypeInfo)
  }

}
