package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.{Join, Project}
import org.apache.calcite.rel.rules.JoinCommuteRule
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeUtil

import scala.collection.JavaConversions.asScalaBuffer

class JoinConditionCommuteRule extends RelOptRule(
  operand(classOf[Join], any),
  "JoinConditionCommuteRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    if (join.getCondition.isAlwaysTrue) {
      return false
    }
    val typeFactory = call.builder().getTypeFactory
    hasEqualsRefsOfDifferentTypes(typeFactory, join.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topProject:Project = call.rel(0)
    val join:Join = call.rel(1)

    // 1. We check if it is a permutation project. If it is
    //    not, or this is the identity, the rule will do nothing
    val topPermutation = topProject.getPermutation
    if (topPermutation == null) return
    if (topPermutation.isIdentity) return

    // 2. We swap the join
    val swapped = JoinCommuteRule.swap(join, true)
    if (swapped == null) return

    // 3. The result should have a project on top, otherwise we
    //    bail out.
    if (swapped.isInstanceOf[Join]) return

    // 4. We check if it is a permutation project. If it is

    val bottomProject = swapped.asInstanceOf[Project]
    val bottomPermutation = bottomProject.getPermutation
    if (bottomPermutation == null) return
    if (bottomPermutation.isIdentity) return

    // 5. If the product of the topPermutation and bottomPermutation yields
    //    the identity, then we can swap the join and remove the project on
    //    top.
    val product = topPermutation.product(bottomPermutation)
    if (!product.isIdentity) return

    // 6. Return the new join as a replacement
    val swappedJoin = bottomProject.getInput(0).asInstanceOf[Join]
    call.transformTo(swappedJoin)
  }

  /**
   * Returns true if two input refs of an equal call have different types in join condition,
   * else false.
   */
  private def hasEqualsRefsOfDifferentTypes(
                                             typeFactory: RelDataTypeFactory,
                                             predicate: RexNode): Boolean = {
    val conjunctions = RelOptUtil.conjunctions(predicate)
    conjunctions.exists {
      case c: RexCall if c.isA(SqlKind.EQUALS) =>
        (c.operands.head, c.operands.last) match {
          case (ref1: RexInputRef, ref2: RexInputRef) =>
            !SqlTypeUtil.equalSansNullability(
              typeFactory,
              ref1.getType,
              ref2.getType)
          case _ => false
        }
      case _ => false
    }
  }
}

object JoinConditionCommuteRule {
  val INSTANCE = new JoinConditionCommuteRule
}

