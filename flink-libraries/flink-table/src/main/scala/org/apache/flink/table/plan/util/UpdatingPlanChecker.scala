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

import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.table.plan.nodes.datastream._

import _root_.scala.collection.JavaConverters._

object UpdatingPlanChecker {

  /** Validates that the plan produces only append changes. */
  def isAppendOnly(plan: RelNode): Boolean = {
    val appendOnlyValidator = new AppendOnlyValidator
    appendOnlyValidator.go(plan)

    appendOnlyValidator.isAppendOnly
  }

  /** Extracts the unique keys of the table produced by the plan. */
  def getUniqueKeyFields(plan: RelNode): Option[Array[String]] = {
    val keyExtractor = new UniqueKeyExtractor
    keyExtractor.go(plan)
    keyExtractor.keys
  }

  private class AppendOnlyValidator extends RelVisitor {

    var isAppendOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: DataStreamRel if s.producesUpdates =>
          isAppendOnly = false
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }

  /** Identifies unique key fields in the output of a RelNode. */
  private class UniqueKeyExtractor extends RelVisitor {

    var keys: Option[Array[String]] = None

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case c: DataStreamCalc =>
          super.visit(node, ordinal, parent)
          // check if input has keys
          if (keys.isDefined) {
            // track keys forward
            val inNames = c.getInput.getRowType.getFieldNames
            val inOutNames = c.getProgram.getNamedProjects.asScala
              .map(p => {
                c.getProgram.expandLocalRef(p.left) match {
                  // output field is forwarded input field
                  case i: RexInputRef => (i.getIndex, p.right)
                  // output field is renamed input field
                  case a: RexCall if a.getKind.equals(SqlKind.AS) =>
                    a.getOperands.get(0) match {
                      case ref: RexInputRef =>
                        (ref.getIndex, p.right)
                      case _ =>
                        (-1, p.right)
                    }
                  // output field is not forwarded from input
                  case _: RexNode => (-1, p.right)
                }
              })
              // filter all non-forwarded fields
              .filter(_._1 >= 0)
              // resolve names of input fields
              .map(io => (inNames.get(io._1), io._2))

            // filter by input keys
            val outKeys = inOutNames.filter(io => keys.get.contains(io._1)).map(_._2)
            // check if all keys have been preserved
            if (outKeys.nonEmpty && outKeys.length == keys.get.length) {
              // all key have been preserved (but possibly renamed)
              keys = Some(outKeys.toArray)
            } else {
              // some (or all) keys have been removed. Keys are no longer unique and removed
              keys = None
            }
          }
        case _: DataStreamOverAggregate =>
          super.visit(node, ordinal, parent)
        // keys are always forwarded by Over aggregate
        case a: DataStreamGroupAggregate =>
          // get grouping keys
          val groupKeys = a.getRowType.getFieldNames.asScala.take(a.getGroupings.length)
          keys = Some(groupKeys.toArray)
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keys
          val groupKeys =
            w.getRowType.getFieldNames.asScala.take(w.getGroupings.length).toArray
          // get window start and end time
          val windowStartEnd = w.getWindowProperties.map(_.name)
          // we have only a unique key if at least one window property is selected
          if (windowStartEnd.nonEmpty) {
            keys = Some(groupKeys ++ windowStartEnd)
          }
        case _: DataStreamRel =>
          // anything else does not forward keys or might duplicate key, so we can stop
          keys = None
      }
    }

  }

}
