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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that coerces the both sides of EQUALS(`=`) operator in Join condition
  * to the same type while sans nullability.
  *
  * <p>For most cases, we already did the type coercion during type validation by implicit
  * type coercion or during sqlNode to relNode conversion, this rule just does a rechecking
  * to ensure a strongly uniform equals type, so that during a HashJoin shuffle we can have
  * the same hashcode of the same value.
  */
class JoinConditionTypeCoerceRule extends RelOptRule(
  operand(classOf[Join], any),
  "JoinConditionTypeCoerceRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    if (join.getCondition.isAlwaysTrue) {
      return false
    }
    val typeFactory = call.builder().getTypeFactory
    hasEqualsRefsOfDifferentTypes(typeFactory, join.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val builder = call.builder()
    val rexBuilder = builder.getRexBuilder
    val typeFactory = builder.getTypeFactory

    val newJoinFilters = mutable.ArrayBuffer[RexNode]()
    val joinFilters = RelOptUtil.conjunctions(join.getCondition)
    joinFilters.foreach {
      case c: RexCall if c.isA(SqlKind.EQUALS) =>
        (c.operands.head, c.operands.last) match {
          case (ref1: RexInputRef, ref2: RexInputRef)
            if !SqlTypeUtil.equalSansNullability(
              typeFactory,
              ref1.getType,
              ref2.getType) =>
            val refList = ref1 :: ref2 :: Nil
            val targetType = typeFactory.leastRestrictive(refList.map(ref => ref.getType))
            if (targetType == null) {
              throw new TableException(
                s"implicit type conversion between" +
                s" ${ref1.getType} and ${ref2.getType} " +
                s"is not supported on join's condition now")
            }
            newJoinFilters += builder.equals(
              rexBuilder.ensureType(targetType, ref1, true),
              rexBuilder.ensureType(targetType, ref2, true))
          case _ =>
            newJoinFilters += c
        }
      case r: RexNode =>
        newJoinFilters += r
    }

    val newCondExp = builder.and(
      FlinkRexUtil.simplify(
        rexBuilder,
        builder.and(newJoinFilters),
        join.getCluster.getPlanner.getExecutor))

    val newJoin = join.copy(
      join.getTraitSet,
      newCondExp,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)

    call.transformTo(newJoin)
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

object JoinConditionTypeCoerceRule {
  val INSTANCE = new JoinConditionTypeCoerceRule
}
