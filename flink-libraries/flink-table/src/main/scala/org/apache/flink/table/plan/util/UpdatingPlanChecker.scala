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

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.table.plan.nodes.datastream._

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.JavaConversions._
import scala.collection.mutable

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
    keyExtractor.visit(plan).map(_.map(_._1).toArray)
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

    // visit() function will return a tuple, the first element is the name of a key field, the
    // second is a group name that is shared by all equivalent key fields. The group names are
    // used to identify same keys, for example: select('pk as pk1, 'pk as pk2), both pk1 and pk2
    // belong to the same group, i.e., pk1. Here we use the lexicographic smallest attribute as
    // the common group id. A node can have keys if it generates the keys by itself or it
    // forwards keys from its input(s).
    def visit(node: RelNode): Option[List[(String, String)]] = {
      node match {
        case c: DataStreamCalc =>
          val inputKeys = visit(node.getInput(0))
          // check if input has keys
          if (inputKeys.isDefined) {
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
            val inputKeysAndOutput = inOutNames
              .filter(io => inputKeys.get.map(e => e._1).contains(io._1))

            val inputKeysMap = inputKeys.get.toMap
            val inOutGroups = inputKeysAndOutput
              .map(e => (inputKeysMap(e._1), e._2)).sorted.reverse.toMap

            // get output keys
            val outputKeys = inputKeysAndOutput
              .map(io => (io._2, inOutGroups(inputKeysMap(io._1))))

            // check if all keys have been preserved
            if (outputKeys.map(_._2).distinct.length == inputKeys.get.map(_._2).distinct.length) {
              // all key have been preserved (but possibly renamed)
              Some(outputKeys.toList)
            } else {
              // some (or all) keys have been removed. Keys are no longer unique and removed
              None
            }
          } else {
            None
          }

        case _: DataStreamOverAggregate =>
          // keys are always forwarded by Over aggregate
          visit(node.getInput(0))
        case a: DataStreamGroupAggregate =>
          // get grouping keys
          val groupKeys = a.getRowType.getFieldNames.asScala.take(a.getGroupings.length)
          Some(groupKeys.map(e => (e, e)).toList)
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keys
          val groupKeys =
            w.getRowType.getFieldNames.asScala.take(w.getGroupings.length).toArray
          // get window start and end time
          val windowStartEnd = w.getWindowProperties.map(_.name)
          // we have only a unique key if at least one window property is selected
          if (windowStartEnd.nonEmpty) {
            val smallestAttribute = windowStartEnd.min
            Some((groupKeys.map(e => (e, e)) ++ windowStartEnd.map((_, smallestAttribute))).toList)
          } else {
            None
          }

        case j: DataStreamJoin =>
          val joinType = j.getJoinType
          joinType match {
            case JoinRelType.INNER => {
              // get key(s) for inner join
              val lInputKeys = visit(j.getLeft)
              val rInputKeys = visit(j.getRight)
              if (lInputKeys.isEmpty || rInputKeys.isEmpty) {
                None
              } else {
                // Output of inner join must have keys if left and right both contain key(s).
                // Key groups from both side will be merged by join equi-predicates
                val lFieldNames: Seq[String] = j.getLeft.getRowType.getFieldNames
                val rFieldNames: Seq[String] = j.getRight.getRowType.getFieldNames
                val lJoinKeys: Seq[String] = j.getJoinInfo.leftKeys.map(lFieldNames.get(_))
                val rJoinKeys: Seq[String] = j.getJoinInfo.rightKeys.map(rFieldNames.get(_))

                getOutputKeysForInnerJoin(
                  lFieldNames ++ rFieldNames,
                  lInputKeys.get ++ rInputKeys.get,
                  lJoinKeys.zip(rJoinKeys).toList
                )
              }
            }
            case _ => throw new UnsupportedOperationException(
              s"An Unsupported JoinType [ $joinType ]")
          }
        case _: DataStreamRel =>
          // anything else does not forward keys, so we can stop
          None
      }
    }

    /**
      * Get output keys for non-window inner join according to it's inputs.
      *
      * @param inNames  Input field names of left and right
      * @param inKeys   Input keys of left and right
      * @param joinKeys JoinKeys of inner join
      * @return Return output keys of inner join
      */
    def getOutputKeysForInnerJoin(
        inNames: Seq[String],
        inKeys: List[(String, String)],
        joinKeys: List[(String, String)])
    : Option[List[(String, String)]] = {

      val nameToGroups = mutable.HashMap.empty[String,String]

      // merge two groups
      def merge(nameA: String, nameB: String): Unit = {
        val ga: String = findGroup(nameA)
        val gb: String = findGroup(nameB)
        if (!ga.equals(gb)) {
          if(ga.compare(gb) < 0) {
            nameToGroups += (gb -> ga)
          } else {
            nameToGroups += (ga -> gb)
          }
        }
      }

      def findGroup(x: String): String = {
        // find the group of x
        var r: String = x
        while (!nameToGroups(r).equals(r)) {
          r = nameToGroups(r)
        }

        // point all name to the group name directly
        var a: String = x
        var b: String = null
        while (!nameToGroups(a).equals(r)) {
          b = nameToGroups(a)
          nameToGroups += (a -> r)
          a = b
        }
        r
      }

      // init groups
      inNames.foreach(e => nameToGroups += (e -> e))
      inKeys.foreach(e => nameToGroups += (e._1 -> e._2))
      // merge groups
      joinKeys.foreach(e => merge(e._1, e._2))
      // make sure all name point to the group name directly
      inNames.foreach(findGroup(_))

      val outputGroups = inKeys.map(e => nameToGroups(e._1)).distinct
      Some(
        inNames
          .filter(e => outputGroups.contains(nameToGroups(e)))
          .map(e => (e, nameToGroups(e)))
          .toList
      )
    }
  }
}
