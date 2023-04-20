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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.alias.ClearJoinHintWithInvalidPropagationShuttle
import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, FlinkRelFactories}
import org.apache.flink.table.planner.hint.FlinkHints

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand, RelOptUtil}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.RelOptUtil.Logic
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.{Filter, JoinRelType}
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.Util

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Planner rule that converts IN and EXISTS into semi-join, converts NOT IN and NOT EXISTS into
 * anti-join.
 *
 * <p>Sub-queries are represented by [[RexSubQuery]] expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated, the wrapped
 * [[RelNode]] will contain a [[RexCorrelVariable]] before the rewrite, and the product of the
 * rewrite will be a [[org.apache.calcite.rel.core.Join]] with SEMI or ANTI join type.
 */
class FlinkSubQueryRemoveRule(
    operand: RelOptRuleOperand,
    relBuilderFactory: RelBuilderFactory,
    description: String)
  extends RelOptRule(operand, relBuilderFactory, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val condition = filter.getCondition

    if (hasUnsupportedSubQuery(condition)) {
      // has some unsupported subquery, such as: subquery connected with OR
      // select * from t1 where t1.a > 10 or t1.b in (select t2.c from t2)
      // TODO supports ExistenceJoin
      return
    }

    val subQueryCall = findSubQuery(condition)
    if (subQueryCall.isEmpty) {
      // ignore scalar query
      return
    }

    val decorrelate = SubQueryDecorrelator.decorrelateQuery(filter)
    if (decorrelate == null) {
      // can't handle the query
      return
    }

    val relBuilder = call.builder.asInstanceOf[FlinkRelBuilder]
    relBuilder.push(filter.getInput) // push join left

    val newCondition = handleSubQuery(subQueryCall.get, condition, relBuilder, decorrelate)
    newCondition match {
      case Some(c) =>
        if (hasCorrelatedExpressions(c)) {
          // some correlated expressions can not be replaced in this rule,
          // so we must keep the VariablesSet for decorrelating later in new filter
          // RelBuilder.filter can not create Filter with VariablesSet arg
          val newFilter = filter.copy(filter.getTraitSet, relBuilder.build(), c)
          relBuilder.push(newFilter)
        } else {
          // all correlated expressions are replaced,
          // so we can create a new filter without any VariablesSet
          relBuilder.filter(c)
        }
        relBuilder.project(fields(relBuilder, filter.getRowType.getFieldCount))
        // the sub query has been replaced with a common node,
        // so hints in it should also be resolved with the same logic in SqlToRelConverter
        val newNode = relBuilder.build
        val nodeWithHint = RelOptUtil.propagateRelHints(newNode, false)
        val nodeWithCapitalizedJoinHints = FlinkHints.capitalizeJoinHints(nodeWithHint)
        val finalNode =
          nodeWithCapitalizedJoinHints.accept(new ClearJoinHintWithInvalidPropagationShuttle)
        call.transformTo(finalNode)
      case _ => // do nothing
    }
  }

  def handleSubQuery(
      subQueryCall: RexCall,
      condition: RexNode,
      relBuilder: FlinkRelBuilder,
      decorrelate: SubQueryDecorrelator.Result): Option[RexNode] = {
    val logic = LogicVisitor.find(Logic.TRUE, ImmutableList.of(condition), subQueryCall)
    if (logic != Logic.TRUE) {
      // this should not happen, none unsupported SubQuery could not reach here
      // this is just for double-check
      return None
    }

    val target = apply(subQueryCall, relBuilder, decorrelate)
    if (target.isEmpty) {
      return None
    }

    val newCondition = replaceSubQuery(condition, subQueryCall, target.get)
    val nextSubQueryCall = findSubQuery(newCondition)
    nextSubQueryCall match {
      case Some(subQuery) => handleSubQuery(subQuery, newCondition, relBuilder, decorrelate)
      case _ => Some(newCondition)
    }
  }

  private def apply(
      subQueryCall: RexCall,
      relBuilder: FlinkRelBuilder,
      decorrelate: SubQueryDecorrelator.Result): Option[RexNode] = {

    val (subQuery: RexSubQuery, withNot: Boolean) = subQueryCall match {
      case s: RexSubQuery => (s, false)
      case c: RexCall => (c.operands.head, true)
    }

    val equivalent = decorrelate.getSubQueryEquivalent(subQuery)

    subQuery.getKind match {
      // IN and NOT IN
      //
      // NOT IN is a NULL-aware (left) anti join e.g. col NOT IN expr Construct the condition.
      // A NULL in one of the conditions is regarded as a positive result;
      // such a row will be filtered out by the Anti-Join operator.
      //
      // Rewrite logic for NOT IN:
      // Expand the NOT IN expression with the NULL-aware semantic to its full form.
      // That is from:
      //   (a1,a2,...) = (b1,b2,...)
      // to
      //   (a1=b1 OR isnull(a1=b1)) AND (a2=b2 OR isnull(a2=b2)) AND ...
      //
      // After that, add back the correlated join predicate(s) in the subquery
      // Example:
      // SELECT ... FROM A WHERE A.A1 NOT IN (SELECT B.B1 FROM B WHERE B.B2 = A.A2 AND B.B3 > 1)
      // will have the final conditions in the ANTI JOIN as
      // (A.A1 = B.B1 OR ISNULL(A.A1 = B.B1)) AND (B.B2 = A.A2)
      case SqlKind.IN =>
        // TODO:
        // Calcite does not support project with correlated expressions.
        // e.g.
        // SELECT b FROM l WHERE (
        // CASE WHEN a IN (SELECT i FROM t1 WHERE l.b = t1.j) THEN 1 ELSE 2 END)
        // IN (SELECT d FROM r)
        //
        // we can not create project with VariablesSet arg, and
        // the result of RelDecorrelator is also wrong.
        if (hasCorrelatedExpressions(subQuery.getOperands: _*)) {
          return None
        }

        val (newRight, joinCondition) = if (equivalent != null) {
          // IN has correlation variables
          (equivalent.getKey, Some(equivalent.getValue))
        } else {
          // IN has no correlation variables
          (subQuery.rel, None)
        }
        // adds projection if the operands of IN contains non-RexInputRef nodes
        // e.g. SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r)
        val (newOperands, newJoinCondition) =
          handleSubQueryOperands(subQuery, joinCondition, relBuilder)
        val leftFieldCount = relBuilder.peek().getRowType.getFieldCount

        relBuilder.push(newRight) // push join right

        val joinConditions = newOperands
          .zip(relBuilder.fields())
          .map {
            case (op, f) =>
              val inCondition = relBuilder.equals(op, RexUtil.shift(f, leftFieldCount))
              if (withNot) {
                relBuilder.or(inCondition, relBuilder.isNull(inCondition))
              } else {
                inCondition
              }
          }
          .toBuffer

        newJoinCondition.foreach(joinConditions += _)

        if (withNot) {
          relBuilder.join(JoinRelType.ANTI, joinConditions)
        } else {
          relBuilder.join(JoinRelType.SEMI, joinConditions)
        }
        Some(relBuilder.literal(true))

      // EXISTS and NOT EXISTS
      case SqlKind.EXISTS =>
        val joinCondition = if (equivalent != null) {
          // EXISTS has correlation variables
          relBuilder.push(equivalent.getKey) // push join right
          require(equivalent.getValue != null)
          equivalent.getValue
        } else {
          // EXISTS has no correlation variables
          //
          // e.g. (table `l` has two columns: `a`, `b`, and table `r` has two columns: `c`, `d`)
          // SELECT * FROM l WHERE EXISTS (SELECT * FROM r)
          // which can be converted to:
          //
          // LogicalProject(a=[$0], b=[$1])
          //  LogicalJoin(condition=[$2], joinType=[semi])
          //    LogicalTableScan(table=[[builtin, default, l]])
          //    LogicalProject($f0=[IS NOT NULL($0)])
          //     LogicalAggregate(group=[{}], m=[MIN($0)])
          //       LogicalProject(i=[true])
          //         LogicalTableScan(table=[[builtin, default, r]])
          //
          // MIN($0) will return null when table `r` is empty,
          // so add LogicalProject($f0=[IS NOT NULL($0)]) to check null value
          val leftFieldCount = relBuilder.peek().getRowType.getFieldCount
          relBuilder.push(subQuery.rel) // push join right
          // adds LogicalProject(i=[true]) to join right
          relBuilder.project(relBuilder.alias(relBuilder.literal(true), "i"))
          // adds LogicalAggregate(group=[{}], agg#0=[MIN($0)]) to join right
          relBuilder.aggregate(relBuilder.groupKey(), relBuilder.min("m", relBuilder.field(0)))
          // adds LogicalProject($f0=[IS NOT NULL($0)]) to check null value
          relBuilder.project(relBuilder.isNotNull(relBuilder.field(0)))
          val fieldType = relBuilder.peek().getRowType.getFieldList.get(0).getType
          // join condition references project result directly
          new RexInputRef(leftFieldCount, fieldType)
        }

        if (withNot) {
          relBuilder.join(JoinRelType.ANTI, joinCondition)
        } else {
          relBuilder.join(JoinRelType.SEMI, joinCondition)
        }
        Some(relBuilder.literal(true))

      case _ => None
    }
  }

  private def fields(builder: RelBuilder, fieldCount: Int): util.List[RexNode] = {
    val projects: util.List[RexNode] = new util.ArrayList[RexNode]()
    (0 until fieldCount).foreach(i => projects.add(builder.field(i)))
    projects
  }

  private def isScalarQuery(n: RexNode): Boolean = n.isA(SqlKind.SCALAR_QUERY)

  private def findSubQuery(node: RexNode): Option[RexCall] = {
    val subQueryFinder = new RexVisitorImpl[Unit](true) {
      override def visitSubQuery(subQuery: RexSubQuery): Unit = {
        if (!isScalarQuery(subQuery)) {
          throw new Util.FoundOne(subQuery)
        }
      }

      override def visitCall(call: RexCall): Unit = {
        call.getKind match {
          case SqlKind.NOT if call.operands.head.isInstanceOf[RexSubQuery] =>
            if (!isScalarQuery(call.operands.head)) {
              throw new Util.FoundOne(call)
            }
          case _ =>
            super.visitCall(call)
        }
      }
    }

    try {
      node.accept(subQueryFinder)
      None
    } catch {
      case e: Util.FoundOne => Some(e.getNode.asInstanceOf[RexCall])
    }
  }

  private def replaceSubQuery(
      condition: RexNode,
      oldSubQueryCall: RexCall,
      replacement: RexNode): RexNode = {
    condition.accept(new RexShuttle() {
      override def visitSubQuery(subQuery: RexSubQuery): RexNode = {
        if (subQuery.equals(oldSubQueryCall)) replacement else subQuery
      }

      override def visitCall(call: RexCall): RexNode = {
        call.getKind match {
          case SqlKind.NOT if call.operands.head.isInstanceOf[RexSubQuery] =>
            if (call.equals(oldSubQueryCall)) replacement else call
          case _ =>
            super.visitCall(call)
        }
      }
    })
  }

  /**
   * Adds projection if the operands of a SubQuery contains non-RexInputRef nodes, and returns
   * SubQuery's new operands and new join condition with new index.
   *
   * e.g. SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r) We will add projection as SEMI join left
   * input, the added projection will pass along fields from the input, and add `a + 1` as new
   * field.
   */
  private def handleSubQueryOperands(
      subQuery: RexSubQuery,
      joinCondition: Option[RexNode],
      relBuilder: RelBuilder): (Seq[RexNode], Option[RexNode]) = {
    val operands = subQuery.getOperands
    // operands is empty or all operands are RexInputRef
    if (operands.isEmpty || operands.forall(_.isInstanceOf[RexInputRef])) {
      return (operands, joinCondition)
    }

    val rexBuilder = relBuilder.getRexBuilder
    val oldLeftNode = relBuilder.peek()
    val oldLeftFieldCount = oldLeftNode.getRowType.getFieldCount
    val newLeftProjects = mutable.ListBuffer[RexNode]()
    val newOperandIndices = mutable.ListBuffer[Int]()
    (0 until oldLeftFieldCount).foreach(newLeftProjects += rexBuilder.makeInputRef(oldLeftNode, _))
    operands.foreach {
      o =>
        var index = newLeftProjects.indexOf(o)
        if (index < 0) {
          index = newLeftProjects.size
          newLeftProjects += o
        }
        newOperandIndices += index
    }

    // adjust join condition after adds new projection
    val newJoinCondition = if (joinCondition.isDefined) {
      val offset = newLeftProjects.size - oldLeftFieldCount
      Some(RexUtil.shift(joinCondition.get, oldLeftFieldCount, offset))
    } else {
      None
    }

    relBuilder.project(newLeftProjects) // push new join left
    val newOperands = newOperandIndices.map(rexBuilder.makeInputRef(relBuilder.peek(), _))
    (newOperands, newJoinCondition)
  }

  /**
   * Check the condition whether contains unsupported SubQuery.
   *
   * Now, we only support single SubQuery or SubQuery connected with AND.
   */
  private def hasUnsupportedSubQuery(condition: RexNode): Boolean = {
    val visitor = new RexVisitorImpl[Unit](true) {
      val stack: util.Deque[SqlKind] = new util.ArrayDeque[SqlKind]()

      private def checkAndConjunctions(call: RexCall): Unit = {
        if (stack.exists(_ ne SqlKind.AND)) {
          throw new Util.FoundOne(call)
        }
      }

      override def visitSubQuery(subQuery: RexSubQuery): Unit = {
        // ignore scalar query
        if (!isScalarQuery(subQuery)) {
          checkAndConjunctions(subQuery)
        }
      }

      override def visitCall(call: RexCall): Unit = {
        call.getKind match {
          case SqlKind.NOT if call.operands.head.isInstanceOf[RexSubQuery] =>
            // ignore scalar query
            if (!isScalarQuery(call.operands.head)) {
              checkAndConjunctions(call)
            }
          case _ =>
            stack.push(call.getKind)
            call.operands.foreach(_.accept(this))
            stack.pop()
        }
      }
    }

    try {
      condition.accept(visitor)
      false
    } catch {
      case _: Util.FoundOne => true
    }
  }

  /** Check nodes' SubQuery whether contains correlated expressions. */
  private def hasCorrelatedExpressions(nodes: RexNode*): Boolean = {
    val relShuttle = new RelShuttleImpl() {
      private val corVarFinder = new RexVisitorImpl[Unit](true) {
        override def visitCorrelVariable(corVar: RexCorrelVariable): Unit = {
          throw new Util.FoundOne(corVar)
        }
      }

      override def visit(filter: LogicalFilter): RelNode = {
        filter.getCondition.accept(corVarFinder)
        super.visit(filter)
      }

      override def visit(join: LogicalJoin): RelNode = {
        join.getCondition.accept(corVarFinder)
        super.visit(join)
      }

      override def visit(project: LogicalProject): RelNode = {
        project.getProjects.foreach(_.accept(corVarFinder))
        super.visit(project)
      }
    }

    val subQueryFinder = new RexVisitorImpl[Unit](true) {
      override def visitSubQuery(subQuery: RexSubQuery): Unit = {
        subQuery.rel.accept(relShuttle)
      }
    }

    nodes.foldLeft(false) {
      case (found, c) =>
        if (!found) {
          try {
            c.accept(subQueryFinder)
            false
          } catch {
            case _: Util.FoundOne => true
          }
        } else {
          true
        }
    }
  }
}

object FlinkSubQueryRemoveRule {

  val FILTER = new FlinkSubQueryRemoveRule(
    operandJ(classOf[Filter], null, RexUtil.SubQueryFinder.FILTER_PREDICATE, any),
    FlinkRelFactories.FLINK_REL_BUILDER,
    "FlinkSubQueryRemoveRule:Filter")

}
