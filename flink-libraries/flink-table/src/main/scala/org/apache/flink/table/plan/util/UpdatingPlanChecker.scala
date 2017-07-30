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
    if (!keyExtractor.visit(plan).isDefined) {
      None
    } else {
      Some(keyExtractor.visit(plan).get.map(ka => ka._1).toArray)
    }
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
  private class UniqueKeyExtractor {

    // visit() function will return a tuple, the first element of tuple is the key, the second is
    // the key's corresponding ancestor. Ancestors are used to identify same keys, for example:
    // select('pk as pk1, 'pk as pk2), both pk1 and pk2 have the same ancestor, i.e., pk.
    // A node having keys means: 1.it generates keys by itself 2.all ancestors from it's upstream
    // nodes have been preserved even though the ancestors have been duplicated.
    def visit(node: RelNode): Option[List[(String, String)]] = {
      node match {
        case c: DataStreamCalc =>
          val keyAncestors = visit(node.getInput(0))
          // check if input has keyAncestors
          if (keyAncestors.isDefined) {
            // track keyAncestors forward
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

            // filter by input keyAncestors
            val outKeyAncesters = inOutNames
              .filter(io => keyAncestors.get.map(e => e._1).contains(io._1))
              .map(io => (io._2, keyAncestors.get.find(ka => ka._1 == io._1).get._2))

            // check if all keyAncestors have been preserved
            if (outKeyAncesters.nonEmpty &&
              outKeyAncesters.map(ka => ka._2).distinct.length ==
                keyAncestors.get.map(ka => ka._2).distinct.length) {
              // all key have been preserved (but possibly renamed)
              Some(outKeyAncesters.toList)
            } else {
              // some (or all) keys have been removed. Keys are no longer unique and removed
              None
            }
          } else {
            None
          }

        case _: DataStreamOverAggregate =>
          // keyAncestors are always forwarded by Over aggregate
          visit(node.getInput(0))
        case a: DataStreamGroupAggregate =>
          // get grouping keyAncestors
          val groupKeys = a.getRowType.getFieldNames.asScala.take(a.getGroupings.length)
          Some(groupKeys.map(e => (e, e)).toList)
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keyAncestors
          val groupKeys =
            w.getRowType.getFieldNames.asScala.take(w.getGroupings.length).toArray
          // get window start and end time
          val windowStartEnd = w.getWindowProperties.map(_.name)
          // we have only a unique key if at least one window property is selected
          if (windowStartEnd.nonEmpty) {
            Some((groupKeys ++ windowStartEnd).map(e => (e, e)).toList)
          } else {
            None
          }

        case j: DataStreamJoin =>
          val leftKeyAncestors = visit(j.getLeft)
          val rightKeyAncestors = visit(j.getRight)
          if (!leftKeyAncestors.isDefined || !rightKeyAncestors.isDefined) {
            None
          } else {
            // both left and right contain keys
            val leftJoinKeys =
              j.getLeft.getRowType.getFieldNames.asScala.zipWithIndex
              .filter(e => j.getJoinInfo.leftKeys.contains(e._2))
              .map(e => e._1)
            val rightJoinKeys =
              j.getRight.getRowType.getFieldNames.asScala.zipWithIndex
                .filter(e => j.getJoinInfo.rightKeys.contains(e._2))
                .map(e => e._1)

            val leftKeys = leftKeyAncestors.get.map(e => e._1)
            val rightKeys = rightKeyAncestors.get.map(e => e._1)

            //1. join key = left key = right key
            if (leftJoinKeys == leftKeys && rightJoinKeys == rightKeys) {
              Some(leftKeyAncestors.get ::: (rightKeyAncestors.get.map(e => (e._1)) zip
                leftKeyAncestors.get.map(e => (e._2))))
            }
            //2. join key = left key
            else if (leftJoinKeys == leftKeys && rightJoinKeys != rightKeys) {
              rightKeyAncestors
            }
            //3. join key = right key
            else if (leftJoinKeys != leftKeys && rightJoinKeys == rightKeys) {
              leftKeyAncestors
            }
            //4. join key not left or right key
            else {
              Some(leftKeyAncestors.get ++ rightKeyAncestors.get)
            }
          }
        case _: DataStreamRel =>
          // anything else does not forward keyAncestors, so we can stop
          None
      }
    }
  }
}
