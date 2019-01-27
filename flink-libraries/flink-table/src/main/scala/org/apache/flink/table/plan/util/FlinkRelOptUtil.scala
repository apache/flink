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
package org.apache.flink.table.plan.util

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.functions.sql.internal.SqlAuxiliaryGroupAggFunction
import org.apache.flink.table.validate.{BuiltInFunctionCatalog, FunctionCatalog}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.util.Pair

import java.io.{PrintWriter, StringWriter}
import java.lang.{Boolean => JBool, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort, String => JString}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.Calendar

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object FlinkRelOptUtil {

  /**
    * Converts a relational expression to a string.
    * This is different from [[RelOptUtil]]#toString on two points:
    * 1. Generated string by this method is in a tree style
    * 2. Generated string by this method may have more information about RelNode, such as
    * resource, memory cost, RelNodeId, retractionTraits.
    *
    * @param rel                the RelNode to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withRelNodeId      whether including ID of RelNode
    * @param withRetractTraits  whether including Retraction Traits of RelNode (only apply to
    *                           StreamPhysicalRel node at present)
    * @return explain plan of RelNode
    */
  def toString(
      rel: RelNode,
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRelNodeId: Boolean = false,
      withRetractTraits: Boolean = false): String = {
    val sw = new StringWriter
    val planWriter = new RelTreeWriterImpl(
      new PrintWriter(sw),
      detailLevel,
      withRelNodeId,
      withRetractTraits)
    rel.explain(planWriter)
    sw.toString
  }

  def getDigest(rel: RelNode, withInput: Boolean = false): String = {
    val sw: StringWriter = new StringWriter
    val pw: RelWriter = new RelWriterImpl(
      new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false) {
      override protected def explain_(
          rel: RelNode, values: util.List[Pair[String, AnyRef]]): Unit = {

        pw.write(rel.getRelTypeName)
        pw.write("(")
        var cnt = 0
        values.foreach { value =>
          value.right match {
            case _: RelNode if !withInput => // ignore
            case _ =>
              if (cnt > 0) {
                pw.write(", ")
              }
              pw.write(value.left + "=[" + value.right + "]")
              cnt += 1
          }
        }
        pw.write(")")
      }
    }
    rel.explain(pw)
    sw.toString
  }

  def getTableConfig(rel: RelNode): TableConfig = {
    Option(rel.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig]))
      .getOrElse(TableConfig.DEFAULT)
  }


  def getFunctionCatalog(rel: RelNode): FunctionCatalog = {
    Option(rel.getCluster.getPlanner.getContext.unwrap(classOf[FunctionCatalog]))
      .getOrElse(BuiltInFunctionCatalog.withBuiltIns())
  }

  /**
    * Get unique field name based on existed `allFieldNames` collection.
    * NOTES: the new unique field name will be added to existed `allFieldNames` collection.
    */
  def buildUniqueFieldName(
      allFieldNames: mutable.Set[String],
      toAddFieldName: String): String = {
    var name: String = toAddFieldName
    var i: Int = 0
    while (allFieldNames.contains(name)) {
      name = toAddFieldName + "_" + i
      i += 1
    }
    allFieldNames.add(name)
    name
  }

  /**
    * Check whether AUXILIARY_GROUP aggCalls is in the front of the given agg's aggCallList,
    * and whether aggCallList contain AUXILIARY_GROUP when the given agg's groupSet is empty
    * or the indicator is true.
    * Returns AUXILIARY_GROUP aggCalls' args and other aggCalls.
    *
    * @param agg aggregate
    * @return returns AUXILIARY_GROUP aggCalls' args and other aggCalls
    */
  def checkAndSplitAggCalls(agg: Aggregate): (Array[Int], Seq[AggregateCall]) = {
    var nonAuxGroupCallsStartIdx = -1

    val aggCalls = agg.getAggCallList
    aggCalls.zipWithIndex.foreach {
      case (call, idx) =>
        if (call.getAggregation == SqlAuxiliaryGroupAggFunction) {
          require(call.getArgList.size == 1)
        }
        if (nonAuxGroupCallsStartIdx >= 0) {
          // the left aggCalls should not be AUXILIARY_GROUP
          require(call.getAggregation != SqlAuxiliaryGroupAggFunction,
                  "AUXILIARY_GROUP should be in the front of aggCall list")
        }
        if (nonAuxGroupCallsStartIdx < 0 &&
          call.getAggregation != SqlAuxiliaryGroupAggFunction) {
          nonAuxGroupCallsStartIdx = idx
        }
    }

    if (nonAuxGroupCallsStartIdx < 0) {
      nonAuxGroupCallsStartIdx = aggCalls.length
    }

    val (auxGroupCalls, otherAggCalls) = aggCalls.splitAt(nonAuxGroupCallsStartIdx)
    if (agg.getGroupCount == 0) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when groupSet is empty")
    }
    if (agg.indicator) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when indicator is true")
    }

    val auxGrouping = auxGroupCalls.map(_.getArgList.head.toInt).toArray
    require(auxGrouping.length + otherAggCalls.length == aggCalls.length)
    (auxGrouping, otherAggCalls)
  }

  def checkAndGetFullGroupSet(agg: Aggregate): Array[Int] = {
    val (auxGroupSet, _) = checkAndSplitAggCalls(agg)
    agg.getGroupSet.toArray ++ auxGroupSet
  }

  /** Get max cnf node limit by context of rel */
  def getMaxCnfNodeCount(rel: RelNode): Int = {
    getTableConfig(rel).getConf.getInteger(TableConfigOptions.SQL_OPTIMIZER_CNF_NODES_LIMIT)
  }

  /**
    * Gets values of RexLiteral
    *
    * @param literal input RexLiteral
    * @return values of the input RexLiteral
    */
  def getLiteralValue(literal: RexLiteral): Comparable[_] = {
    if (literal.isNull) {
      null
    } else {
      val literalType = literal.getType
      literalType.getSqlTypeName match {
        case BOOLEAN => RexLiteral.booleanValue(literal)
        case TINYINT => literal.getValueAs(classOf[JByte])
        case SMALLINT => literal.getValueAs(classOf[JShort])
        case INTEGER => literal.getValueAs(classOf[Integer])
        case BIGINT => literal.getValueAs(classOf[JLong])
        case FLOAT => literal.getValueAs(classOf[JFloat])
        case DOUBLE => literal.getValueAs(classOf[JDouble])
        case DECIMAL => literal.getValue3.asInstanceOf[BigDecimal]
        case VARCHAR | CHAR => literal.getValueAs(classOf[JString])

        // temporal types
        case DATE =>
          new Date(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIME =>
          new Time(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIMESTAMP =>
          new Timestamp(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case _ =>
          throw new IllegalArgumentException(s"Literal type $literalType is not supported!")
      }
    }
  }

  /**
    * Partitions the [[RexNode]] in two [[RexNode]] according to a predicate.
    * The result is a pair of RexNode: the first RexNode consists of RexNode that satisfy the
    * predicate and the second RexNode consists of RexNode that don't.
    *
    * For simple condition which is not AND, OR, NOT, it is completely satisfy the predicate or not.
    *
    * For complex condition Ands, partition each operands of ANDS recursively, then
    * merge the RexNode which satisfy the predicate as the first part, merge the rest parts as the
    * second part.
    *
    * For complex condition ORs, try to pull up common factors among ORs first, if the common
    * factors is not A ORs, then simplify the question to partition the common factors expression;
    * else the input condition is completely satisfy the predicate or not based on whether all
    * its operands satisfy the predicate or not.
    *
    * For complex condition NOT, it is completely satisfy the predicate or not based on whether its
    * operand satisfy the predicate or not.
    *
    * @param expr            the expression to partition
    * @param rexBuilder      rexBuilder
    * @param predicate       the specified predicate on which to partition
    * @return a pair of RexNode: the first RexNode consists of RexNode that satisfy the predicate
    *         and the second RexNode consists of RexNode that don't
    */
  def partition(
      expr: RexNode,
      rexBuilder: RexBuilder,
      predicate: RexNode => JBool): (Option[RexNode], Option[RexNode]) = {
    val condition = pushNotToLeaf(expr, rexBuilder)
    val (left: Option[RexNode], right: Option[RexNode]) = condition.getKind match {
      case AND =>
        val (leftExprs, rightExprs) = partition(
          condition.asInstanceOf[RexCall].operands, rexBuilder, predicate)
        if (leftExprs.isEmpty) {
          (None, Option(condition))
        } else {
          val l = RexUtil.composeConjunction(rexBuilder, leftExprs.asJava, false)
          if (rightExprs.isEmpty) {
            (Option(l), None)
          } else {
            val r = RexUtil.composeConjunction(rexBuilder, rightExprs.asJava, false)
            (Option(l), Option(r))
          }
        }
      case OR =>
        val e = RexUtil.pullFactors(rexBuilder, condition)
        e.getKind match {
          case OR =>
            val (leftExprs, rightExprs) = partition(
              condition.asInstanceOf[RexCall].operands, rexBuilder, predicate)
            if (leftExprs.isEmpty || rightExprs.nonEmpty) {
              (None, Option(condition))
            } else {
              val l = RexUtil.composeDisjunction(rexBuilder, leftExprs.asJava, false)
              (Option(l), None)
            }
          case _ =>
            partition(e, rexBuilder, predicate)
        }
      case NOT =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        partition(operand, rexBuilder, predicate) match {
          case (Some(_), None) => (Option(condition), None)
          case (_, _) => (None, Option(condition))
        }
      case IS_TRUE =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        partition(operand, rexBuilder, predicate)
      case IS_FALSE =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        val newCondition = pushNotToLeaf(operand, rexBuilder, needReverse = true)
        partition(newCondition, rexBuilder, predicate)
      case _ =>
        if (predicate(condition)) {
          (Option(condition), None)
        } else {
          (None, Option(condition))
        }
    }
    (convertRexNodeIfAlwaysTrue(left), convertRexNodeIfAlwaysTrue(right))
  }

  private def partition(
      exprs: Iterable[RexNode],
      rexBuilder: RexBuilder,
      predicate: RexNode => JBool): (Iterable[RexNode], Iterable[RexNode]) = {
    val leftExprs = mutable.ListBuffer[RexNode]()
    val rightExprs = mutable.ListBuffer[RexNode]()
    exprs.foreach(expr => partition(expr, rexBuilder, predicate) match {
      case (Some(first), Some(second)) =>
        leftExprs += first
        rightExprs += second
      case (None, Some(rest)) =>
        rightExprs += rest
      case (Some(interested), None) =>
        leftExprs += interested
    })
    (leftExprs, rightExprs)
  }

  private def convertRexNodeIfAlwaysTrue(expr: Option[RexNode]): Option[RexNode] = {
    expr match {
      case Some(rex) if rex.isAlwaysTrue => None
      case _ => expr
    }
  }

  private def pushNotToLeaf(expr: RexNode,
      rexBuilder: RexBuilder,
      needReverse: Boolean = false): RexNode = (expr.getKind, needReverse) match {
    case (AND, true) | (OR, false) => 
      val convertedExprs = expr.asInstanceOf[RexCall].operands
                           .map(pushNotToLeaf(_, rexBuilder, needReverse))
      RexUtil.composeDisjunction(rexBuilder,  convertedExprs, false)
    case (AND, false) | (OR, true) =>
      val convertedExprs = expr.asInstanceOf[RexCall].operands
                           .map(pushNotToLeaf(_, rexBuilder, needReverse))
      RexUtil.composeConjunction(rexBuilder, convertedExprs, false)
    case (NOT, _) =>
      val child = expr.asInstanceOf[RexCall].operands.head
      pushNotToLeaf(child, rexBuilder, !needReverse)
    case (_, true) if expr.isInstanceOf[RexCall] =>
      val negatedExpr = RexUtil.negate(rexBuilder, expr.asInstanceOf[RexCall])
      if (negatedExpr != null) negatedExpr else RexUtil.not(expr)
    case (_, true) => RexUtil.not(expr)
    case (_, false) => expr
  }

  /**
    * An RexVisitor to judge whether the RexNode is related to the specified index InputRef
    */
  class ColumnRelatedVisitor(index: Int) extends RexVisitorImpl[JBool](true) {

    override def visitInputRef(inputRef: RexInputRef): JBool = inputRef.getIndex == index

    override def visitLiteral(literal: RexLiteral): JBool = true

    override def visitCall(call: RexCall): JBool = {
      call.operands.forall(operand => {
        val isRelated = operand.accept(this)
        isRelated != null && isRelated
      })
    }
  }

}
