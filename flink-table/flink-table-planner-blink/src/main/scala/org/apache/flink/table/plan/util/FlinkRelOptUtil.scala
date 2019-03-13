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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.table.api.TableException

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.core.{AggregateCall, JoinInfo}
import org.apache.calcite.rel.{RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.mapping.IntPair

import java.util

import scala.collection.mutable

object FlinkRelOptUtil {

  /**
    * Get unique field name based on existed `allFieldNames` collection.
    * NOTES: the new unique field name will be added to existed `allFieldNames` collection.
    */
  def buildUniqueFieldName(
      allFieldNames: util.Set[String],
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
    * Returns indices of group functions.
    */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: Seq[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
    * Returns limit start value (never null).
    */
  def getLimitStart(offset: RexNode): Long = if (offset != null) RexLiteral.intValue(offset) else 0L

  /**
    * Returns limit end value (never null).
    */
  def getLimitEnd(offset: RexNode, fetch: RexNode): Long = {
    if (fetch != null) {
      getLimitStart(offset) + RexLiteral.intValue(fetch)
    } else {
      Long.MaxValue
    }
  }

  /**
    * Converts [[Direction]] to [[Order]].
    */
  def directionToOrder(direction: Direction): Order = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }

  /**
    * Gets sort key indices, sort orders and null directions from given field collations.
    */
  def getSortKeysAndOrders(
      fieldCollations: Seq[RelFieldCollation]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val fieldMappingDirections = fieldCollations.map {
      c => (c.getFieldIndex, directionToOrder(c.getDirection))
    }
    val keys = fieldMappingDirections.map(_._1)
    val orders = fieldMappingDirections.map(_._2 == Order.ASCENDING)
    val nullsIsLast = fieldCollations.map(_.nullDirection).map {
      case RelFieldCollation.NullDirection.LAST => true
      case RelFieldCollation.NullDirection.FIRST => false
      case RelFieldCollation.NullDirection.UNSPECIFIED =>
        throw new TableException(s"Do not support UNSPECIFIED for null order.")
    }.toArray

    deduplicateSortKeys(keys.toArray, orders.toArray, nullsIsLast)
  }

  /**
    * Removes duplicate sort keys.
    */
  private def deduplicateSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val keySet = new mutable.HashSet[Int]
    val keyBuffer = new mutable.ArrayBuffer[Int]
    val orderBuffer = new mutable.ArrayBuffer[Boolean]
    val nullsIsLastBuffer = new mutable.ArrayBuffer[Boolean]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        keyBuffer += keys(i)
        orderBuffer += orders(i)
        nullsIsLastBuffer += nullsIsLast(i)
      }
    }
    (keyBuffer.toArray, orderBuffer.toArray, nullsIsLastBuffer.toArray)
  }

  /**
    * Check and get join left and right keys.
    */
  def checkAndGetJoinKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmptyKey: Boolean = false): (Array[Int], Array[Int]) = {
    // get the equality keys
    val leftKeys = mutable.ArrayBuffer.empty[Int]
    val rightKeys = mutable.ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmptyKey) {
        (leftKeys.toArray, rightKeys.toArray)
      } else {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tleft: ${left.toString}\n\tright: ${right.toString}\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach { pair =>
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys += pair.source
          rightKeys += pair.target
        } else {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}\n" +
              "please re-check the join statement.")
        }
      }
      (leftKeys.toArray, rightKeys.toArray)
    }
  }

  /**
    * Creates a [[JoinInfo]] by analyzing a condition.
    *
    * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]],
    * the only difference is that the methods could return `filterNulls`.
    */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining = RelOptUtil.splitJoinCondition(
      left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

}
