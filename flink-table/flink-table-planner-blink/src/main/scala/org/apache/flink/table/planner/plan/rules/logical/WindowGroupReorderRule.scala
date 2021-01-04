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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.{LogicalProject, LogicalWindow}
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.RexInputRef

import java.util
import java.util.{Collections, Comparator}

import scala.collection.JavaConversions._

/**
  * Planner rule that makes the over window groups which have the same shuffle keys and order keys
  * together.
  */
class WindowGroupReorderRule extends RelOptRule(
  operand(classOf[LogicalWindow],
    operand(classOf[RelNode], any)),
  "ExchangeWindowGroupRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val window: LogicalWindow = call.rel(0)
    window.groups.size() > 1
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val window: LogicalWindow = call.rel(0)
    val input: RelNode = call.rel(1)
    val oldGroups: util.List[Group] = new util.ArrayList(window.groups)
    val sequenceGroups: util.List[Group] = new util.ArrayList(window.groups)

    sequenceGroups.sort(new Comparator[Group] {
      override def compare(o1: Group, o2: Group): Int = {
        val keyComp = o1.keys.compareTo(o2.keys)
        if (keyComp == 0) {
          compareRelCollation(o1.orderKeys, o2.orderKeys)
        } else {
          keyComp
        }
      }
    })

    if (!sequenceGroups.equals(oldGroups) && !sequenceGroups.reverse.equals(oldGroups)) {
      var offset = input.getRowType.getFieldCount
      val aggTypeIndexes = oldGroups.map { group =>
        val aggCount = group.aggCalls.size()
        val typeIndexes = (0 until aggCount).map(_ + offset).toArray
        offset += aggCount
        typeIndexes
      }

      offset = input.getRowType.getFieldCount
      val mapToOldTypeIndexes = (0 until offset).toArray ++
        sequenceGroups.flatMap { newGroup =>
          val aggCount = newGroup.aggCalls.size()
          val oldIndex = oldGroups.indexOf(newGroup)
          offset += aggCount
          (0 until aggCount).map {
            aggIndex => aggTypeIndexes(oldIndex)(aggIndex)
          }
        }.toArray[Int]

      val oldRowTypeFields = window.getRowType.getFieldList
      val newFieldList = new util.ArrayList[util.Map.Entry[String, RelDataType]]
      mapToOldTypeIndexes.foreach { index =>
        newFieldList.add(oldRowTypeFields.get(index))
      }
      val intermediateRowType = window.getCluster.getTypeFactory.createStructType(newFieldList)
      val newLogicalWindow = LogicalWindow.create(
        window.getCluster.getPlanner.emptyTraitSet(),
        input,
        window.constants,
        intermediateRowType,
        sequenceGroups)

      val mapToNewTypeIndexes = mapToOldTypeIndexes.zipWithIndex.sortBy(_._1)

      val projects = mapToNewTypeIndexes.map { index =>
        new RexInputRef(index._2, newFieldList.get(index._2).getValue)
      }
      val project = LogicalProject.create(
        newLogicalWindow,
        Collections.emptyList[RelHint](),
        projects.toList,
        window.getRowType)
      call.transformTo(project)
    }
  }

  private def compareRelCollation(o1: RelCollation, o2: RelCollation): Int = {
    val comp = o1.compareTo(o2)
    if (comp == 0) {
      val collations1 = o1.getFieldCollations
      val collations2 = o2.getFieldCollations
      for (index <- 0 until collations1.length) {
        val collation1 = collations1(index)
        val collation2 = collations2(index)
        val direction = collation1.direction.shortString.compareTo(collation2.direction.shortString)
        if (direction == 0) {
          val nullDirection = collation1.nullDirection.nullComparison.compare(
            collation2.nullDirection.nullComparison)
          if (nullDirection != 0) {
            return nullDirection
          }
        } else {
          return direction
        }
      }
    }
    comp
  }
}

object WindowGroupReorderRule {
  val INSTANCE = new WindowGroupReorderRule
}
