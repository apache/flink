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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.stats.SkewInfoInternal

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Optimize the join with skewed table, pick the skewed values to join separately,
  * and then union the result with the original join.
  *
  * After this optimization, the tree should be like:
  * TS -> (FIL "skewed rows") * -> RS -
  *                                    \
  *                                      ->   JOIN
  *                                    /           \
  * TS -> (FIL "skewed rows") * -> RS -             \
  *                                                  \
  *                                                    ->  UNION -> ..
  *                                                  /
  * TS -> (FIL "no skewed rows") * -> RS -          /
  *                                       \        /
  *                                        -> JOIN
  *                                        /
  * TS -> (FIL "no skewed rows") * -> RS -
  *
  * <p>NOTE: The skewed join must be a separate stage (or use RULE_SEQUENCE) because if filter
  * is pushed down, it cannot determine if Skewed Join should be generated again.
  */
class SkewedJoinRule
    extends RelOptRule(
      operand(classOf[Join], operand(classOf[RelNode], any)),
      "SkewedJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val left = join.getLeft
    val right = join.getRight
    val joinInfo = join.analyzeCondition
    val mq = FlinkRelMetadataQuery.reuseOrCreate(join.getCluster.getMetadataQuery)
    val leftSkewInfo = mq.getSkewInfo(left)
    val rightSkewInfo = mq.getSkewInfo(right)

    def noSkewValues(node: RelNode, key: Integer, skewInfo: SkewInfoInternal) = {
      skewInfo.skewInfo.get(node.getRowType.getFieldNames.indices.get(key)) match {
        case Some(values) => values.isEmpty
        case _ => true
      }
    }

    val leftNoSkew = leftSkewInfo == null ||
        joinInfo.leftKeys.forall(noSkewValues(left, _, leftSkewInfo))
    val rightNoSkew = rightSkewInfo == null ||
        joinInfo.rightKeys.forall(noSkewValues(right, _, rightSkewInfo))
    !(leftNoSkew && rightNoSkew)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val left = join.getLeft
    val right = join.getRight
    val joinInfo = join.analyzeCondition
    val meta = FlinkRelMetadataQuery.reuseOrCreate(join.getCluster.getMetadataQuery)
    val leftSkewInfo = meta.getSkewInfo(left)
    val rightSkewInfo = meta.getSkewInfo(right)
    val leftSkew =
      if (leftSkewInfo == null) Map[Int, Seq[AnyRef]]() else leftSkewInfo.skewInfo
    val rightSkew =
      if (rightSkewInfo == null) Map[Int, Seq[AnyRef]]() else rightSkewInfo.skewInfo
    val leftIndice = left.getRowType.getFieldNames.indices
    val rightIndice = right.getRowType.getFieldNames.indices

    // get skew pairs
    val projectMap = new mutable.HashMap[IntPair, Seq[AnyRef]]
    joinInfo.pairs().foreach(
      (pair) =>
        if (leftSkew.contains(leftIndice(pair.source)) ||
            rightSkew.contains(rightIndice(pair.target))) {
          projectMap.put(pair,
            leftSkew.getOrElse(leftIndice(pair.source), Seq[AnyRef]()) ++
                rightSkew.getOrElse(rightIndice(pair.target), Seq[AnyRef]()))
        }
    )

    // generate filters
    val relBuilder = call.builder()
    val rexBuilder = relBuilder.getRexBuilder
    val leftEqualFilters = new mutable.ArrayBuffer[RexNode]
    val leftNotEqualFilters = new mutable.ArrayBuffer[RexNode]
    val rightEqualFilters = new mutable.ArrayBuffer[RexNode]
    val rightNotEqualFilters = new mutable.ArrayBuffer[RexNode]
    projectMap.foreach{
      case (pair, values) =>
        values.foreach {
          (v) =>
            def addRexNode(
                relNode: RelNode,
                index: Int,
                equalFilters: mutable.ArrayBuffer[RexNode],
                notEqualFilters: mutable.ArrayBuffer[RexNode]) = {
              val inputRef = rexBuilder.makeInputRef(relNode, index)
              val literal = rexBuilder.makeLiteral(
                v, relNode.getRowType.getFieldList.get(index).getType, true)
              equalFilters += relBuilder.equals(inputRef, literal)
              notEqualFilters += relBuilder.notEquals(inputRef, literal)
            }

            addRexNode(left, pair.source, leftEqualFilters, leftNotEqualFilters)
            addRexNode(right, pair.target, rightEqualFilters, rightNotEqualFilters)
        }
    }

    // generate new joins
    // We have now pick out all possible skewed values, so the condition is connected by OR.
    // We do not support pick combination Keys only.
    val joinForSkewV = join.copy(join.getTraitSet, List(
      relBuilder.push(left).filter(relBuilder.or(leftEqualFilters: _*)).build(),
      relBuilder.push(right).filter(relBuilder.or(rightEqualFilters: _*)).build()))

    val joinForNonSkewV = join.copy(join.getTraitSet, List(
      relBuilder.push(left).filter(leftNotEqualFilters: _*).build(),
      relBuilder.push(right).filter(rightNotEqualFilters: _*).build()))

    // union all
    relBuilder.push(joinForSkewV)
    relBuilder.push(joinForNonSkewV)
    call.transformTo(relBuilder.union(true).build())
  }
}

object SkewedJoinRule {
  val INSTANCE = new SkewedJoinRule
}
