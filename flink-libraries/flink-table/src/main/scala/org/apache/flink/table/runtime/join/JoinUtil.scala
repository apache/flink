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

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.{SqlFloorFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlIntervalQualifier, SqlKind}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenerator, ExpressionReducer}
import org.apache.flink.table.plan.schema.{RowSchema, TimeIndicatorRelDataType}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * An util class to help analyze and build join code .
  */
object JoinUtil {

  /**
    * check if the join case is stream join stream
    *
    * @param  condition   other condtion include time-condition
    * @param  inputType   left and right connect stream type
    */
  private[flink] def isStreamStreamJoin(
    condition: RexNode,
    inputType: RelDataType) = {

    def isExistTumble(expr: RexNode): Boolean = {
      expr match {
        case c: RexCall =>
          c.getOperator match {
            case _: SqlFloorFunction =>
              c.getOperands.map(analyzeSingleConditionTerm(_, 0, inputType)).exists(_.size > 0)
            case SqlStdOperatorTable.TUMBLE =>
              c.getOperands.map(analyzeSingleConditionTerm(_, 0, inputType)).exists(_.size > 0)
            case _ =>
              c.getOperands.map(isExistTumble(_)).exists(_ == true)
          }
        case _ => false
      }
    }

    val isExistTimeIndicator = analyzeSingleConditionTerm(condition, 0, inputType).size > 0
    val isExistTumbleExpr = isExistTumble(condition)

    !isExistTumbleExpr && isExistTimeIndicator
  }

  /**
    * Analyze time-condtion to get time boundary for each stream and get the time type
    * and return remain condition.
    *
    * @param  condition   other condtion include time-condition
    * @param  leftLogicalFieldCnt left stream logical field num
    * @param  leftPhysicalFieldCnt left stream physical field num
    * @param  inputType   left and right connect stream type
    * @param  rexBuilder   util to build rexNode
    * @param  config      table environment config
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
      throw new TableException("There only can and must have 2 time conditions.")
    }

    // extract time offset from the time indicator conditon
    val streamTimeOffsets =
      timeTerms.map(x => extractTimeOffsetFromCondition(x._3, x._2, rexBuilder, config))

    val (leftTableOffset, rightTableOffset) =
      streamTimeOffsets match {
        case Seq((x, true), (y, false)) => (x, y)
        case Seq((x, false), (y, true)) => (y, x)
        case _ =>
          throw new TableException("Both input need time boundary.")
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

    (timeTerms.get(0)._1, leftTableOffset, rightTableOffset, remainCondition)
  }

  /**
   * Split the join conditions into time condition and non-time condition
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
        val timeIndicators = analyzeSingleConditionTerm(c, leftFieldCount, inputType)
        timeIndicators match {
          case Seq() =>
            (Seq(), Seq(c))
          case Seq(v1, v2) =>
            if (v1._1 != v2._1) {
              throw new TableException("The time indicators for each input should be the same.")
            }
            if (v1._2 == v2._2) {
              throw new TableException("Both input's time indicators is needed.")
            }
            (Seq((v1._1, v1._2, c)), Seq())
          case _ =>
            throw new TableException(
              "There only can and must have one time indicators for each input.")
        }
      case other =>
        val timeIndicators = analyzeSingleConditionTerm(other, leftFieldCount, inputType)
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
   */
  def analyzeSingleConditionTerm(
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
        c.operands.map(analyzeSingleConditionTerm(_, leftFieldCount, inputType)).reduce(_++_)
      case _ => Seq()
    }
  }

  /**
    * Extract time offset and determain which table the offset belong to
    */
  def extractTimeOffsetFromCondition(
      timeTerm: RexNode,
      isLeftExprBelongLeftTable: Boolean,
      rexBuilder: RexBuilder,
      config: TableConfig) = {

    val timeCall: RexCall = timeTerm.asInstanceOf[RexCall]

    val (tmpTimeOffset: Long, isLeftTableTimeOffset: Boolean) =
      timeTerm.getKind match {
        // e.g a.proctime > b.proctime - 5 sec, we need to store stream a.
        // the left expr(a) belong to left table, so the offset belong to left table
        case kind @ (SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL) =>
          (reduceTimeExpression(
            timeCall.operands.get(1),
            timeCall.operands.get(0),
            rexBuilder,
            config), isLeftExprBelongLeftTable)
        // e.g a.proctime < b.proctime + 5 sec, we need to store stream b.
        case kind @ (SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL) =>
          (reduceTimeExpression(
            timeCall.operands.get(0),
            timeCall.operands.get(1),
            rexBuilder,
            config), !isLeftExprBelongLeftTable)
        case _ => 0
      }

    val timeOffset =
      // only preceding offset need to store records
      if (tmpTimeOffset < 0)
        // determain the boudary value
        if (timeTerm.getKind == SqlKind.LESS_THAN || timeTerm.getKind == SqlKind.GREATER_THAN) {
          -tmpTimeOffset - 1
        } else {
          -tmpTimeOffset
        }
      else 0

    (timeOffset, isLeftTableTimeOffset)
  }

  /**
    * Calcute the time boundary. Replace the rowtime/proctime with zero literal.
    * For example:
    *  a.proctime - inteval '1' second > b.proctime - interval '1' second - interval '2' second
    *  |-----------left--------------|   |-------------------right---------------------------\
    * then the boundary of a is right - left:
    *  ((0 - 1000) - 2000) - (0 - 1000) = -2000(-preceding, +following)
    */
  private def reduceTimeExpression(
    leftNode: RexNode,
    rightNode: RexNode,
    rexBuilder: RexBuilder,
    config: TableConfig): Long = {

    /**
      * replace the rowtime/proctime with zero literal.
      * Because calculation between timestamp can only be TIMESTAMP +/- INTERVAL
      * so such as b.proctime + interval '1' hour - a.proctime
      * will be translate into TIMESTAMP(0) + interval '1' hour - interval '0' second
      */
    def replaceTimeFieldWithLiteral(
      expr: RexNode,
      isTimeStamp: Boolean): RexNode = {

      expr match {
        case c: RexCall =>
          // replace in call operands
          val newOps = c.operands.map(o => replaceTimeFieldWithLiteral(o, isTimeStamp))
          rexBuilder.makeCall(c.getType, c.getOperator, newOps)
        case i: RexInputRef if FlinkTypeFactory.isTimeIndicatorType(i.getType) && isTimeStamp =>
          // replace with timestamp
          rexBuilder.makeZeroLiteral(expr.getType)
        case i: RexInputRef if FlinkTypeFactory.isTimeIndicatorType(i.getType) && !isTimeStamp =>
          // replace with time interval
          val sqlQualifier =
            new SqlIntervalQualifier(
              TimeUnit.SECOND,
              0,
              null,
              0,
              SqlParserPos.ZERO)
          rexBuilder.makeIntervalLiteral(JBigDecimal.ZERO, sqlQualifier)
        case _: RexInputRef =>
          throw new TableException("Time join condition may only reference time indicator fields.")
        case _ => expr
      }
    }

    val replLeft = replaceTimeFieldWithLiteral(leftNode, true)
    val replRight = replaceTimeFieldWithLiteral(rightNode, false)
    val literalRex = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, replLeft, replRight)

    val exprReducer = new ExpressionReducer(config)
    val originList = new util.ArrayList[RexNode]()
    originList.add(literalRex)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    reduceList.get(0) match {
      case call: RexCall =>
        call.getOperands.get(0).asInstanceOf[RexLiteral].getValue2.asInstanceOf[Long]
      case literal: RexLiteral =>
        literal.getValue2.asInstanceOf[Long]
      case _ =>
        throw TableException("Equality join time condition only support constant.")
    }
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
