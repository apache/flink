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
import java.util.EnumSet

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlIntervalQualifier, SqlKind}
import org.apache.flink.api.common.functions.{FilterFunction, FlatJoinFunction}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenException, CodeGenerator, ExpressionReducer}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


object JoinUtil {

  /**
    * Analyze time-condtion to get time boundary for each stream and get the time type
    * and return condition without time-condition.
    *
    * @param  condition   other condtion include time-condition
    * @param  leftFieldCount left stream fields count
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
      config: TableConfig): (RelDataType, Long, Long, RexNode) = {
    // analyze the time-conditon to get greate and less condition,
    // make sure left stream field in the left of the condition
    // e.g b.proctime > a.proctime - 1 will be translate to a.proctime - 1 < b.proctime
    val greateConditions = new util.ArrayList[TimeSingleCondition]()
    val lessConditions = new util.ArrayList[TimeSingleCondition]()
    analyzeTimeCondition(condition, greateConditions,
      lessConditions, leftLogicalFieldCnt, inputType)
    if (greateConditions.size != lessConditions.size
        || greateConditions.size > 1
        || greateConditions.size == 0) {
      throw TableException(
        "Equality join time conditon should have proctime or rowtime indicator."
      )
    }

    val greatCond = greateConditions.get(0)
    val lessCond = lessConditions.get(0)
    if (greatCond.timeType != lessCond.timeType) {
      throw TableException(
        "Equality join time conditon should all use proctime or all use rowtime."
      )
    }

    var leftStreamWindowSize: Long = 0
    var rightStreamWindowSize: Long = 0

    // only a.proctime > b.proctime - interval '1' hour need to store a stream
    val timeLiteral: RexLiteral =
        reduceTimeExpression(greatCond.rightExpr, greatCond.leftExpr, rexBuilder, config)
    leftStreamWindowSize = timeLiteral.getValue2.asInstanceOf[Long]
    // only need to store past records
    if (leftStreamWindowSize < 0) {
      leftStreamWindowSize = -leftStreamWindowSize
      if (!greatCond.isEqual) {
        leftStreamWindowSize -= 1
      }
    } else {
      leftStreamWindowSize = 0
    }

    // only a.proctime < b.proctime + interval '1' hour need to store b stream
    val timeLiteral2: RexLiteral =
        reduceTimeExpression(lessCond.leftExpr, lessCond.rightExpr, rexBuilder, config)
    rightStreamWindowSize = timeLiteral2.getValue2.asInstanceOf[Long]
    // only need to store past records
    if (rightStreamWindowSize < 0) {
      rightStreamWindowSize = -rightStreamWindowSize
      if (!lessCond.isEqual) {
        rightStreamWindowSize -= 1
      }
    } else {
      rightStreamWindowSize = 0
    }

    // get condition without time-condition
    // e.g a.price > b.price and a.proctime between b.proctime and b.proctime + interval '1' hour
    // will return a.price > b.price and true and true
    var conditionWithoutTime = removeTimeCondition(
      condition,
      greatCond.originCall,
      lessCond.originCall,
      rexBuilder,
      leftLogicalFieldCnt,
      leftPhysicalFieldCnt)

    // reduce the expression
    // true and ture => true, otherwise keep the origin expression
    try {
      val exprReducer = new ExpressionReducer(config)
      val originList = new util.ArrayList[RexNode]()
      originList.add(conditionWithoutTime)
      val reduceList = new util.ArrayList[RexNode]()
      exprReducer.reduce(rexBuilder, originList, reduceList)
      conditionWithoutTime = reduceList.get(0)
    } catch {
      case _ : CodeGenException => // ignore
    }

    (greatCond.timeType, leftStreamWindowSize, rightStreamWindowSize, conditionWithoutTime)
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
    otherCondition: RexNode,
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

    // if other condition is literal(true), then output the result directly
    val body = if (otherCondition.isAlwaysTrue) {
      s"""
         |${conversion.code}
         |${generator.collectorTerm}.collect(${conversion.resultTerm});
         |""".stripMargin
    }
    else {
      val condition = generator.generateExpression(otherCondition)
      s"""
         |${condition.code}
         |if (${condition.resultTerm}) {
         |  ${conversion.code}
         |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
         |}
         |""".stripMargin
    }

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Row, Row, Row]],
      body,
      returnType.physicalTypeInfo)

    genFunction
  }

  private case class TimeSingleCondition(
      timeType: RelDataType,
      leftExpr: RexNode,
      rightExpr: RexNode,
      isEqual: Boolean,
      originCall: RexNode)

  val COMPARISON: util.Set[SqlKind] = EnumSet.of(
    SqlKind.LESS_THAN,
    SqlKind.GREATER_THAN,
    SqlKind.GREATER_THAN_OR_EQUAL,
    SqlKind.LESS_THAN_OR_EQUAL)

  val EQUI_COMPARISON: util.Set[SqlKind] = EnumSet.of(
    SqlKind.GREATER_THAN_OR_EQUAL,
    SqlKind.LESS_THAN_OR_EQUAL)

  val LESS_COMPARISON: util.Set[SqlKind] = EnumSet.of(
    SqlKind.LESS_THAN,
    SqlKind.LESS_THAN_OR_EQUAL)

  val GREAT_COMPARISON: util.Set[SqlKind] = EnumSet.of(
    SqlKind.GREATER_THAN,
    SqlKind.GREATER_THAN_OR_EQUAL)

  /**
    * Analyze time-conditon to divide all time-condition into great and less condition
    */
  private def analyzeTimeCondition(
    condition: RexNode,
    greatCondition: util.List[TimeSingleCondition],
    lessCondition: util.List[TimeSingleCondition],
    leftFieldCount: Int,
    inputType: RelDataType): Unit = {
    if (condition.isInstanceOf[RexCall]) {
      val call: RexCall = condition.asInstanceOf[RexCall]
      call.getKind match {
        case SqlKind.AND =>
          var i = 0
          while (i < call.getOperands.size) {
            analyzeTimeCondition(
              call.getOperands.get(i),
              greatCondition,
              lessCondition,
              leftFieldCount,
              inputType)
            i += 1
          }
        case kind if kind.belongsTo(COMPARISON) =>
          // analyze left expression
          val (isExistTimeIndicator1, timeType1, isLeftStreamAttr1) =
            analyzeTimeExpression(call.getOperands.get(0), leftFieldCount, inputType)

          // make sure proctime/rowtime exist
          if (isExistTimeIndicator1) {
            // analyze right expression
            val (isExistTimeIndicator2, timeType2, isLeftStreamAttr2) =
            analyzeTimeExpression(call.getOperands.get(1), leftFieldCount, inputType)
            if (!isExistTimeIndicator2) {
              throw TableException(
                "Equality join time conditon should include time indicator both side."
              )
            } else if (timeType1 != timeType2) {
              throw TableException(
                "Equality join time conditon should include same time indicator each side."
              )
            } else if (isLeftStreamAttr1 == isLeftStreamAttr2) {
              throw TableException(
                "Equality join time conditon should include both two streams's time indicator."
              )
            } else {
              val isGreate: Boolean = kind.belongsTo(GREAT_COMPARISON)
              val isEqual: Boolean = kind.belongsTo(EQUI_COMPARISON)
              (isGreate, isLeftStreamAttr1) match {
                case (true, true) =>
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(0), call.getOperands.get(1), isEqual, call)
                  greatCondition.add(newCond)
                case (true, false) =>
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(1), call.getOperands.get(0), isEqual, call)
                  lessCondition.add(newCond)
                case (false, true) =>
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(0), call.getOperands.get(1), isEqual, call)
                  lessCondition.add(newCond)
                case (false, false) =>
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(1), call.getOperands.get(0), isEqual, call)
                  greatCondition.add(newCond)

              }
            }
          }
        case _ =>
      }
    }
  }

  /**
    * Analyze time-expression(b.proctime + interval '1' hour) to check whether is valid
    * and get the time predicate type(proctime or rowtime)
    */
  private def analyzeTimeExpression(
     expression: RexNode,
     leftFieldCount: Int,
     inputType: RelDataType): (Boolean, RelDataType, Boolean) = {

    var timeType: RelDataType = null
    var isExistTimeIndicator = false
    var isLeftStreamAttr = false
    if (expression.isInstanceOf[RexInputRef]) {
      val idx = expression.asInstanceOf[RexInputRef].getIndex
      timeType = inputType.getFieldList.get(idx).getType
      timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
          isExistTimeIndicator = true
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
          isExistTimeIndicator = true
        case _ =>
          isExistTimeIndicator = false
      }

      if (idx < leftFieldCount) {
        isLeftStreamAttr = true
      }
    } else if (expression.isInstanceOf[RexCall]) {
      val call: RexCall = expression.asInstanceOf[RexCall]
      var i = 0
      while (i < call.getOperands.size) {
        val (curIsExistSysTimeAttr, curTimeType, curIsLeftStreamAttr) =
          analyzeTimeExpression(call.getOperands.get(i), leftFieldCount, inputType)
        if (isExistTimeIndicator && curIsExistSysTimeAttr) {
          throw TableException(
            s"Equality join time conditon can not include duplicate {$timeType} attribute."
          )
        }
        if (curIsExistSysTimeAttr) {
          isExistTimeIndicator = curIsExistSysTimeAttr
          timeType = curTimeType
          isLeftStreamAttr = curIsLeftStreamAttr
        }

        i += 1
      }

    }
    (isExistTimeIndicator, timeType, isLeftStreamAttr)
  }

  /**
    * Calcute the time boundary. Replace the rowtime/proctime with zero literal.
    * such as:
    *  a.proctime - inteval '1' second > b.proctime - interval '1' second - interval '2' second
    *  |-----------left--------------|   |-------------------right---------------------------\
    * then the boundary of a is right - left:
    *  ((0 - 1000) - 2000) - (0 - 1000) = -2000
    */
  private def reduceTimeExpression(
    leftNode: RexNode,
    rightNode: RexNode,
    rexBuilder: RexBuilder,
    config: TableConfig): RexLiteral = {

    val replLeft = replaceTimeIndicatorWithLiteral(leftNode, rexBuilder, true)
    val replRight = replaceTimeIndicatorWithLiteral(rightNode, rexBuilder,false)
    val literalRex = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, replLeft, replRight)

    val exprReducer = new ExpressionReducer(config)
    val originList = new util.ArrayList[RexNode]()
    originList.add(literalRex)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    reduceList.get(0) match {
      case call: RexCall => call.getOperands.get(0).asInstanceOf[RexLiteral]
      case literal: RexLiteral => literal
      case _ =>
        throw TableException(
          s"Equality join time condition only support constant."
        )

    }
  }

  /**
    * replace the rowtime/proctime with zero literal.
    * Because calculation between timestamp can only be TIMESTAMP +/- INTERVAL
    * so such as b.proctime + interval '1' hour - a.proctime
    * will be translate into TIMESTAMP(0) + interval '1' hour - interval '0' second
    */
  private def replaceTimeIndicatorWithLiteral(
    expr: RexNode,
    rexBuilder: RexBuilder,
    isTimeStamp: Boolean): RexNode = {
    if (expr.isInstanceOf[RexCall]) {
      val call: RexCall = expr.asInstanceOf[RexCall]
      var i = 0
      val operands = new util.ArrayList[RexNode]
      while (i < call.getOperands.size) {
        val newRex =
          replaceTimeIndicatorWithLiteral(call.getOperands.get(i), rexBuilder, isTimeStamp)
        operands.add(newRex)
        i += 1
      }
      rexBuilder.makeCall(call.getType, call.getOperator, operands)
    } else if (expr.isInstanceOf[RexInputRef]) {
      if (isTimeStamp) {
        // replace with timestamp
        rexBuilder.makeZeroLiteral(expr.getType)
      } else {
        // replace with time interval
        val sqlQualifier = new SqlIntervalQualifier(TimeUnit.SECOND, 0, null, 0, SqlParserPos.ZERO)
        rexBuilder.makeIntervalLiteral(JBigDecimal.ZERO, sqlQualifier)
      }
    } else {
      expr
    }
  }

  /**
   * replace the time-condition with true literal.
   */
  private def removeTimeCondition(
    expr: RexNode,
    timeExpr1: RexNode,
    timeExpr2: RexNode,
    rexBuilder: RexBuilder,
    leftLogicalFieldsCnt: Int,
    leftPhysicFieldsCnt: Int
  ): RexNode = {
    if (expr == timeExpr1 || expr == timeExpr2) {
      rexBuilder.makeLiteral(true)
    } else if (expr.isInstanceOf[RexCall]) {
      val call: RexCall = expr.asInstanceOf[RexCall]
      var i = 0
      val operands = new util.ArrayList[RexNode]
      while (i < call.getOperands.size) {
        val newRex = removeTimeCondition(call.getOperands.get(i),
          timeExpr1, timeExpr2, rexBuilder, leftLogicalFieldsCnt, leftPhysicFieldsCnt)
        operands.add(newRex)
        i += 1
      }
      rexBuilder.makeCall(call.getType, call.getOperator, operands)
    } else if (expr.isInstanceOf[RexInputRef]) {
      // get the physical index
      val inputRef = expr.asInstanceOf[RexInputRef]
      if (inputRef.getIndex >= leftLogicalFieldsCnt) {
        rexBuilder.makeInputRef(inputRef.getType,
          inputRef.getIndex - leftLogicalFieldsCnt + leftPhysicFieldsCnt)
          .asInstanceOf[RexNode]
      } else {
        expr
      }
    } else {
      expr
    }
  }

}
