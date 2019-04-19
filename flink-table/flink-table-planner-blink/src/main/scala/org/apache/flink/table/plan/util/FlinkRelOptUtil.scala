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

import org.apache.flink.table.api.{PlannerConfigOptions, TableConfig}
import org.apache.flink.table.calcite.{FlinkContext, FlinkPlannerImpl, FlinkTypeFactory}
import org.apache.flink.table.{JBoolean, JByte, JDouble, JFloat, JLong, JShort}
import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan.RelOptUtil.InputFinder
import org.apache.calcite.plan.{RelOptUtil, Strong}
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
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
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
      withRowType)
    rel.explain(planWriter)
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
    tableConfig.getConf.getInteger(PlannerConfigOptions.SQL_OPTIMIZER_CNF_NODES_LIMIT)
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
    * Simplifies outer joins if filter above would reject nulls.
    *
    * NOTES: This method should be deleted when upgrading to a new calcite version
    * which contains CALCITE-2969.
    *
    * @param joinRel Join
    * @param aboveFilters Filters from above
    * @param joinType Join type, can not be inner join
    */
  def simplifyJoin(
      joinRel: RelNode,
      aboveFilters: ImmutableList[RexNode],
      joinType: JoinRelType): JoinRelType = {
    // No need to simplify if only first input output.
    if (!joinType.projectsRight()) {
      return joinType
    }
    val nTotalFields = joinRel.getRowType.getFieldCount
    val nSysFields = 0
    val nFieldsLeft = joinRel.getInputs.get(0).getRowType.getFieldCount
    val nFieldsRight = joinRel.getInputs.get(1).getRowType.getFieldCount
    assert(nTotalFields == nSysFields + nFieldsLeft + nFieldsRight)

    // set the reference bitmaps for the left and right children
    val leftBitmap = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft)
    val rightBitmap = ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields)

    var result = joinType
    for (filter <- aboveFilters) {
      if (joinType.generatesNullsOnLeft && Strong.isNotTrue(filter, leftBitmap)) {
        result = result.cancelNullsOnLeft
      }
      if (joinType.generatesNullsOnRight && Strong.isNotTrue(filter, rightBitmap)) {
        result = result.cancelNullsOnRight
      }
      if (joinType eq JoinRelType.INNER) {
        return result
      }
    }
    result
  }

  /**
    * Classifies filters according to where they should be processed. They
    * either stay where they are, are pushed to the join (if they originated
    * from above the join), or are pushed to one of the children. Filters that
    * are pushed are added to list passed in as input parameters.
    *
    * NOTES: This method should be deleted when upgrading to a new calcite version
    * which contains CALCITE-2969.
    *
    * @param joinRel      join node
    * @param filters      filters to be classified
    * @param joinType     join type
    * @param pushInto     whether filters can be pushed into the ON clause
    * @param pushLeft     true if filters can be pushed to the left
    * @param pushRight    true if filters can be pushed to the right
    * @param joinFilters  list of filters to push to the join
    * @param leftFilters  list of filters to push to the left child
    * @param rightFilters list of filters to push to the right child
    * @return whether at least one filter was pushed
    */
  def classifyFilters(
      joinRel: RelNode,
      filters: util.List[RexNode],
      joinType: JoinRelType,
      pushInto: Boolean,
      pushLeft: Boolean,
      pushRight: Boolean,
      joinFilters: util.List[RexNode],
      leftFilters: util.List[RexNode],
      rightFilters: util.List[RexNode]): Boolean = {
    val rexBuilder = joinRel.getCluster.getRexBuilder
    val joinFields = joinRel.getRowType.getFieldList
    val nTotalFields = joinFields.size
    val nSysFields = 0 // joinRel.getSystemFieldList().size();
    val leftFields = joinRel.getInputs.get(0).getRowType.getFieldList
    val nFieldsLeft = leftFields.size
    val rightFields = joinRel.getInputs.get(1).getRowType.getFieldList
    val nFieldsRight = rightFields.size

    assert(nTotalFields == (if (joinType.projectsRight()) {
      nSysFields + nFieldsLeft + nFieldsRight
    } else {
      // SEMI/ANTI
      nSysFields + nFieldsLeft
    }))

    // set the reference bitmaps for the left and right children
    val leftBitmap = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft)
    val rightBitmap = ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields)

    val filtersToRemove = new util.ArrayList[RexNode]

    filters.foreach { filter =>
      val inputFinder = InputFinder.analyze(filter)
      val inputBits = inputFinder.inputBitSet.build
      // REVIEW - are there any expressions that need special handling
      // and therefore cannot be pushed?
      // filters can be pushed to the left child if the left child
      // does not generate NULLs and the only columns referenced in
      // the filter originate from the left child
      if (pushLeft && leftBitmap.contains(inputBits)) {
        // ignore filters that always evaluate to true
        if (!filter.isAlwaysTrue) {
          // adjust the field references in the filter to reflect
          // that fields in the left now shift over by the number
          // of system fields
          val shiftedFilter = shiftFilter(
            nSysFields,
            nSysFields + nFieldsLeft,
            -nSysFields,
            rexBuilder,
            joinFields,
            nTotalFields,
            leftFields,
            filter)
          leftFilters.add(shiftedFilter)
        }
        filtersToRemove.add(filter)

        // filters can be pushed to the right child if the right child
        // does not generate NULLs and the only columns referenced in
        // the filter originate from the right child
      } else if (pushRight && rightBitmap.contains(inputBits)) {
        if (!filter.isAlwaysTrue) {
          // that fields in the right now shift over to the left;
          // since we never push filters to a NULL generating
          // child, the types of the source should match the dest
          // so we don't need to explicitly pass the destination
          // fields to RexInputConverter
          val shiftedFilter = shiftFilter(
            nSysFields + nFieldsLeft,
            nTotalFields,
            -(nSysFields + nFieldsLeft),
            rexBuilder,
            joinFields,
            nTotalFields,
            rightFields,
            filter)
          rightFilters.add(shiftedFilter)
        }
        filtersToRemove.add(filter)
      } else {
        // If the filter can't be pushed to either child and the join
        // is an inner join, push them to the join if they originated
        // from above the join
        if ((joinType eq JoinRelType.INNER) && pushInto) {
          if (!joinFilters.contains(filter)) {
            joinFilters.add(filter)
          }
          filtersToRemove.add(filter)
        }
      }
    }
    // Remove filters after the loop, to prevent concurrent modification.
    if (!filtersToRemove.isEmpty) {
      filters.removeAll(filtersToRemove)
    }
    // Did anything change?
    !filtersToRemove.isEmpty
  }

  private def shiftFilter(
      start: Int,
      end: Int,
      offset: Int,
      rexBuilder: RexBuilder,
      joinFields: util.List[RelDataTypeField],
      nTotalFields: Int,
      rightFields: util.List[RelDataTypeField],
      filter: RexNode): RexNode = {
    val adjustments = new Array[Int](nTotalFields)
    (start until end).foreach {
      i => adjustments(i) = offset
    }
    filter.accept(
      new RelOptUtil.RexInputConverter(
        rexBuilder,
        joinFields,
        rightFields,
        adjustments)
    )
  }

  /**
    * Pushes down expressions in "equal" join condition.
    *
    * NOTES: This method should be deleted when upgrading to a new calcite version
    * which contains CALCITE-2969.
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

}
