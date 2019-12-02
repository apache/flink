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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkPlannerImpl, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchInterval, MiniBatchMode}
import org.apache.flink.table.planner.{JBoolean, JByte, JDouble, JFloat, JLong, JShort}

import com.google.common.collect.Lists
import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.{RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexLiteral, RexNode, RexUtil, RexVisitorImpl}
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.mapping.Mappings
import org.apache.calcite.util.{ImmutableBitSet, Pair, Util}
import org.apache.commons.math3.util.ArithmeticUtils

import java.io.{PrintWriter, StringWriter}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.Calendar

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * FlinkRelOptUtil provides utility methods for use in optimizing RelNodes.
  */
object FlinkRelOptUtil {

  /**
    * Converts a relational expression to a string.
    * This is different from [[RelOptUtil]]#toString on two points:
    * 1. Generated string by this method is in a tree style
    * 2. Generated string by this method may have more information about RelNode, such as
    * RelNode id, retractionTraits.
    *
    * @param rel                the RelNode to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withIdPrefix       whether including ID of RelNode as prefix
    * @param withRetractTraits  whether including Retraction Traits of RelNode (only apply to
    * StreamPhysicalRel node at present)
    * @param withRowType        whether including output rowType
    * @return explain plan of RelNode
    */
  def toString(
      rel: RelNode,
      detailLevel: SqlExplainLevel = SqlExplainLevel.DIGEST_ATTRIBUTES,
      withIdPrefix: Boolean = false,
      withRetractTraits: Boolean = false,
      withRowType: Boolean = false): String = {
    if (rel == null) {
      return null
    }
    val sw = new StringWriter
    val planWriter = new RelTreeWriterImpl(
      new PrintWriter(sw),
      detailLevel,
      withIdPrefix,
      withRetractTraits,
      withRowType,
      withTreeStyle = true)
    rel.explain(planWriter)
    sw.toString
  }

  /**
    * Gets the digest for a rel tree.
    *
    * The digest of RelNode should contain the result of RelNode#explain method, retraction traits
    * (for StreamPhysicalRel) and RelNode's row type.
    *
    * Row type is part of the digest for the rare occasion that similar
    * expressions have different types, e.g.
    * "WITH
    * t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as BIGINT)),
    * t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
    * SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b"
    *
    * the physical plan is:
    * {{{
    *  HashJoin(where=[=(b, b0)], join=[a, b, a0, b0], joinType=[InnerJoin],
    *    isBroadcast=[true], build=[right])
    *  :- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
    *  :  +- Exchange(distribution=[hash[a]])
    *  :     +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
    *  :        +- Calc(select=[CAST(a) AS a, b])
    *  :           +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
    *  +- Exchange(distribution=[broadcast])
    *     +- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
    *        +- Exchange(distribution=[hash[a]])
    *           +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
    *              +- Calc(select=[CAST(a) AS a, b])
    *                 +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
    * }}}
    *
    * The sub-plan of `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])`
    * are different because `CAST(a) AS a` has different types, where one is BIGINT type
    * and another is DOUBLE type.
    *
    * If use the result of `RelOptUtil.toString(aggregate, SqlExplainLevel.DIGEST_ATTRIBUTES)`
    * on `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])` as digest,
    * we will get incorrect result. So rewrite `explain_` method of `RelWriterImpl` to
    * add row-type to digest value.
    *
    * @param rel rel node tree
    * @return The digest of given rel tree.
    */
  def getDigest(rel: RelNode): String = {
    val sw = new StringWriter
    rel.explain(new RelTreeWriterImpl(
      new PrintWriter(sw),
      explainLevel = SqlExplainLevel.DIGEST_ATTRIBUTES,
      // ignore id, only contains RelNode's attributes
      withIdPrefix = false,
      // add retraction traits to digest for StreamPhysicalRel node
      withRetractTraits = true,
      // add row type to digest to avoid corner case that similar
      // expressions have different types
      withRowType = true,
      // ignore tree style, only contains RelNode's attributes
      withTreeStyle = false))
    sw.toString
  }

  /**
    * Returns the null direction if not specified.
    *
    * @param direction Direction that a field is ordered in.
    * @return default null direction
    */
  def defaultNullDirection(direction: Direction): NullDirection = {
    FlinkPlannerImpl.defaultNullCollation match {
      case NullCollation.FIRST => NullDirection.FIRST
      case NullCollation.LAST => NullDirection.LAST
      case NullCollation.LOW =>
        direction match {
          case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => NullDirection.FIRST
          case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => NullDirection.LAST
          case _ => NullDirection.UNSPECIFIED
        }
      case NullCollation.HIGH =>
        direction match {
          case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => NullDirection.LAST
          case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => NullDirection.FIRST
          case _ => NullDirection.UNSPECIFIED
        }
    }
  }

  /**
    * Creates a field collation with default direction.
    *
    * @param fieldIndex 0-based index of field being sorted
    * @return the field collation with default direction and given field index.
    */
  def ofRelFieldCollation(fieldIndex: Int): RelFieldCollation = {
    new RelFieldCollation(
      fieldIndex,
      FlinkPlannerImpl.defaultCollationDirection,
      defaultNullDirection(FlinkPlannerImpl.defaultCollationDirection))
  }

  /**
    * Creates a field collation.
    *
    * @param fieldIndex    0-based index of field being sorted
    * @param direction     Direction of sorting
    * @param nullDirection Direction of sorting of nulls
    * @return the field collation.
    */
  def ofRelFieldCollation(
      fieldIndex: Int,
      direction: RelFieldCollation.Direction,
      nullDirection: RelFieldCollation.NullDirection): RelFieldCollation = {
    new RelFieldCollation(fieldIndex, direction, nullDirection)
  }

  def getTableConfigFromContext(rel: RelNode): TableConfig = {
    rel.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
  }

  /** Get max cnf node limit by context of rel */
  def getMaxCnfNodeCount(rel: RelNode): Int = {
    val tableConfig = getTableConfigFromContext(rel)
    tableConfig.getConfiguration.getInteger(FlinkRexUtil.TABLE_OPTIMIZER_CNF_NODES_LIMIT)
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
        case VARCHAR | CHAR => literal.getValueAs(classOf[String])

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
    * Pushes down expressions in "equal" join condition.
    *
    * NOTES: This method should be deleted when CALCITE-3171 is fixed.
    *
    * <p>For example, given
    * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
    * "emp" that computes the expression
    * "emp.deptno + 1". The resulting join condition is a simple combination
    * of AND, equals, and input fields, plus the remaining non-equal conditions.
    *
    * @param originalJoin Join whose condition is to be pushed down
    * @param relBuilder Factory to create project operator
    */
  def pushDownJoinConditions(originalJoin: Join, relBuilder: RelBuilder): RelNode = {
    var joinCond: RexNode = originalJoin.getCondition
    val joinType: JoinRelType = originalJoin.getJoinType

    val extraLeftExprs: util.List[RexNode] = new util.ArrayList[RexNode]
    val extraRightExprs: util.List[RexNode] = new util.ArrayList[RexNode]
    val leftCount: Int = originalJoin.getLeft.getRowType.getFieldCount
    val rightCount: Int = originalJoin.getRight.getRowType.getFieldCount

    // You cannot push a 'get' because field names might change.
    //
    // Pushing sub-queries is OK in principle (if they don't reference both
    // sides of the join via correlating variables) but we'd rather not do it
    // yet.
    if (!containsGet(joinCond) && RexUtil.SubQueryFinder.find(joinCond) == null) {
      joinCond = pushDownEqualJoinConditions(
        joinCond, leftCount, rightCount, extraLeftExprs, extraRightExprs)
    }
    relBuilder.push(originalJoin.getLeft)
    if (!extraLeftExprs.isEmpty) {
      val fields: util.List[RelDataTypeField] = relBuilder.peek.getRowType.getFieldList
      val pairs: util.List[Pair[RexNode, String]] = new util.AbstractList[Pair[RexNode, String]]() {
        override def size: Int = leftCount + extraLeftExprs.size

        override def get(index: Int): Pair[RexNode, String] = if (index < leftCount) {
          val field: RelDataTypeField = fields.get(index)
          Pair.of(new RexInputRef(index, field.getType), field.getName)
        }
        else Pair.of(extraLeftExprs.get(index - leftCount), null)
      }
      relBuilder.project(Pair.left(pairs), Pair.right(pairs))
    }

    relBuilder.push(originalJoin.getRight)
    if (!extraRightExprs.isEmpty) {
      val fields: util.List[RelDataTypeField] = relBuilder.peek.getRowType.getFieldList
      val newLeftCount: Int = leftCount + extraLeftExprs.size
      val pairs: util.List[Pair[RexNode, String]] = new util.AbstractList[Pair[RexNode, String]]() {
        override def size: Int = rightCount + extraRightExprs.size

        override def get(index: Int): Pair[RexNode, String] = if (index < rightCount) {
          val field: RelDataTypeField = fields.get(index)
          Pair.of(new RexInputRef(index, field.getType), field.getName)
        }
        else Pair.of(RexUtil.shift(extraRightExprs.get(index - rightCount), -newLeftCount), null)
      }
      relBuilder.project(Pair.left(pairs), Pair.right(pairs))
    }

    val right: RelNode = relBuilder.build
    val left: RelNode = relBuilder.build
    relBuilder.push(originalJoin.copy(originalJoin.getTraitSet, joinCond, left, right, joinType,
      originalJoin.isSemiJoinDone))

    // handle SEMI/ANTI join here
    var mapping: Mappings.TargetMapping = null
    if (!originalJoin.getJoinType.projectsRight()) {
      if (!extraLeftExprs.isEmpty) {
        mapping = Mappings.createShiftMapping(leftCount + extraLeftExprs.size, 0, 0, leftCount)
      }
    } else {
      if (!extraLeftExprs.isEmpty || !extraRightExprs.isEmpty) {
        mapping = Mappings.createShiftMapping(
          leftCount + extraLeftExprs.size + rightCount + extraRightExprs.size,
          0, 0, leftCount, leftCount, leftCount + extraLeftExprs.size, rightCount)
      }
    }

    if (mapping != null) {
      relBuilder.project(relBuilder.fields(mapping.inverse))
    }
    relBuilder.build
  }

  private def containsGet(node: RexNode) = try {
    node.accept(new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (call.getOperator eq RexBuilder.GET_OPERATOR) {
          throw Util.FoundOne.NULL
        }
        super.visitCall(call)
      }
    })
    false
  } catch {
    case _: Util.FoundOne =>
      true
  }

  /**
    * Pushes down parts of a join condition.
    *
    * <p>For example, given
    * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
    * "emp" that computes the expression
    * "emp.deptno + 1". The resulting join condition is a simple combination
    * of AND, equals, and input fields.
    */
  private def pushDownEqualJoinConditions(
      node: RexNode,
      leftCount: Int,
      rightCount: Int,
      extraLeftExprs: util.List[RexNode],
      extraRightExprs: util.List[RexNode]): RexNode =
    node.getKind match {
      case AND | EQUALS =>
        val call = node.asInstanceOf[RexCall]
        val list = new util.ArrayList[RexNode]
        val operands = Lists.newArrayList(call.getOperands)
        // do not use `operands.zipWithIndex.foreach`
        operands.indices.foreach { i =>
          val operand = operands.get(i)
          val left2 = leftCount + extraLeftExprs.size
          val right2 = rightCount + extraRightExprs.size
          val e = pushDownEqualJoinConditions(
            operand, leftCount, rightCount, extraLeftExprs, extraRightExprs)
          val remainingOperands = Util.skip(operands, i + 1)
          val left3 = leftCount + extraLeftExprs.size
          fix(remainingOperands, left2, left3)
          fix(list, left2, left3)
          list.add(e)
        }

        if (!(list == call.getOperands)) {
          call.clone(call.getType, list)
        } else {
          call
        }
      case OR | INPUT_REF | LITERAL | NOT => node
      case _ =>
        if (node.accept(new TimeIndicatorExprFinder)) {
          node
        } else {
          val bits = RelOptUtil.InputFinder.bits(node)
          val mid = leftCount + extraLeftExprs.size
          Side.of(bits, mid) match {
            case Side.LEFT =>
              fix(extraRightExprs, mid, mid + 1)
              extraLeftExprs.add(node)
              new RexInputRef(mid, node.getType)
            case Side.RIGHT =>
              val index2 = mid + rightCount + extraRightExprs.size
              extraRightExprs.add(node)
              new RexInputRef(index2, node.getType)
            case _ => node
          }
        }
    }

  private def fix(operands: util.List[RexNode], before: Int, after: Int): Unit = {
    if (before == after) {
      return
    }
    operands.indices.foreach { i =>
      val node = operands.get(i)
      operands.set(i, RexUtil.shift(node, before, after - before))
    }
  }

  /**
    * Categorizes whether a bit set contains bits left and right of a line.
    */
  private object Side extends Enumeration {
    type Side = Value
    val LEFT, RIGHT, BOTH, EMPTY = Value

    private[plan] def of(bitSet: ImmutableBitSet, middle: Int): Side = {
      val firstBit = bitSet.nextSetBit(0)
      if (firstBit < 0) {
        return EMPTY
      }
      if (firstBit >= middle) {
        return RIGHT
      }
      if (bitSet.nextSetBit(middle) < 0) {
        return LEFT
      }
      BOTH
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
      predicate: RexNode => JBoolean): (Option[RexNode], Option[RexNode]) = {
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
      predicate: RexNode => JBoolean): (Iterable[RexNode], Iterable[RexNode]) = {
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
      RexUtil.composeDisjunction(rexBuilder, convertedExprs, false)
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
  class ColumnRelatedVisitor(index: Int) extends RexVisitorImpl[JBoolean](true) {

    override def visitInputRef(inputRef: RexInputRef): JBoolean = inputRef.getIndex == index

    override def visitLiteral(literal: RexLiteral): JBoolean = true

    override def visitCall(call: RexCall): JBoolean = {
      call.operands.forall(operand => {
        val isRelated = operand.accept(this)
        isRelated != null && isRelated
      })
    }
  }

  /**
    * An RexVisitor to find whether this is a call on a time indicator field.
    */
  class TimeIndicatorExprFinder extends RexVisitorImpl[Boolean](true) {
    override def visitInputRef(inputRef: RexInputRef): Boolean = {
      FlinkTypeFactory.isTimeIndicatorType(inputRef.getType)
    }
  }

  /**
    * Merge two MiniBatchInterval as a new one.
    *
    * The Merge Logic:  MiniBatchMode: (R: rowtime, P: proctime, N: None), I: Interval
    * Possible values:
    * - (R, I = 0): operators that require watermark (window excluded).
    * - (R, I > 0): window / operators that require watermark with minibatch enabled.
    * - (R, I = -1): existing window aggregate
    * - (P, I > 0): unbounded agg with minibatch enabled.
    * - (N, I = 0): no operator requires watermark, minibatch disabled
    * ------------------------------------------------
    * |    A        |    B        |   merged result
    * ------------------------------------------------
    * | R, I_1 == 0 | R, I_2      |  R, gcd(I_1, I_2)
    * ------------------------------------------------
    * | R, I_1 == 0 | P, I_2      |  R, I_2
    * ------------------------------------------------
    * | R, I_1 > 0  | R, I_2      |  R, gcd(I_1, I_2)
    * ------------------------------------------------
    * | R, I_1 > 0  | P, I_2      |  R, I_1
    * ------------------------------------------------
    * | R, I_1 = -1 | R, I_2      |  R, I_1
    * ------------------------------------------------
    * | R, I_1 = -1 | P, I_2      |  R, I_1
    * ------------------------------------------------
    * | P, I_1      | R, I_2 == 0 |  R, I_1
    * ------------------------------------------------
    * | P, I_1      | R, I_2 > 0  |  R, I_2
    * ------------------------------------------------
    * | P, I_1      | P, I_2 > 0  |  P, I_1
    * ------------------------------------------------
    */
  def mergeMiniBatchInterval(
      interval1: MiniBatchInterval,
      interval2: MiniBatchInterval): MiniBatchInterval = {
    if (interval1 == MiniBatchInterval.NO_MINIBATCH ||
      interval2 == MiniBatchInterval.NO_MINIBATCH) {
      return MiniBatchInterval.NO_MINIBATCH
    }
    interval1.mode match {
      case MiniBatchMode.None => interval2
      case MiniBatchMode.RowTime =>
        interval2.mode match {
          case MiniBatchMode.None => interval1
          case MiniBatchMode.RowTime =>
            val gcd = ArithmeticUtils.gcd(interval1.interval, interval2.interval)
            MiniBatchInterval(gcd, MiniBatchMode.RowTime)
          case MiniBatchMode.ProcTime =>
            if (interval1.interval == 0) {
              MiniBatchInterval(interval2.interval, MiniBatchMode.RowTime)
            } else {
              interval1
            }
        }
      case MiniBatchMode.ProcTime =>
        interval2.mode match {
          case MiniBatchMode.None | MiniBatchMode.ProcTime => interval1
          case MiniBatchMode.RowTime =>
            if (interval2.interval > 0) {
              interval2
            } else {
              MiniBatchInterval(interval1.interval, MiniBatchMode.RowTime)
            }
        }
    }
  }

}
