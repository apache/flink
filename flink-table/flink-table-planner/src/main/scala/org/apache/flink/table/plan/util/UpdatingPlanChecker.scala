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
import org.apache.flink.table.expressions.ProctimeAttribute
import org.apache.flink.table.plan.nodes.datastream._

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
  def getUniqueKeyFields(plan: RelNode, sinkFieldNames: Array[String]): Option[Array[String]] = {
    val relFieldNames = plan.getRowType.getFieldNames
    getUniqueKeyGroups(plan).map(_.map(r => sinkFieldNames(relFieldNames.indexOf(r._1))).toArray)
  }

  /** Extracts the unique keys and groups of the table produced by the plan. */
  def getUniqueKeyGroups(plan: RelNode): Option[Seq[(String, String)]] = {
    val keyExtractor = new UniqueKeyExtractor
    keyExtractor.visit(plan)
  }

  private class AppendOnlyValidator extends RelVisitor {

    var isAppendOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: DataStreamRel if s.producesUpdates || s.producesRetractions =>
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
    def visit(node: RelNode): Option[Seq[(String, String)]] = {
      node match {
        case c: DataStreamCalc =>
          val inputKeys = visit(node.getInput(0))
          // check if input has keys
          if (inputKeys.isDefined) {
            // track keys forward
            val inNames = c.getInput.getRowType.getFieldNames
            val inOutNames = c.getProgram.getNamedProjects
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
            val inOutGroups = inputKeysAndOutput.sorted.reverse
              .map(e => (inputKeysMap(e._1), e._2))
              .toMap

            // get output keys
            val outputKeys = inputKeysAndOutput
              .map(io => (io._2, inOutGroups(inputKeysMap(io._1))))

            // check if all keys have been preserved
            if (outputKeys.map(_._2).distinct.length == inputKeys.get.map(_._2).distinct.length) {
              // all key have been preserved (but possibly renamed)
              Some(outputKeys)
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
          val groupKeys = a.getRowType.getFieldNames.take(a.getGroupings.length)
          Some(groupKeys.map(e => (e, e)))
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keys
          val groupKeys =
            w.getRowType.getFieldNames.take(w.getGroupings.length).toArray
          // proctime is not a valid key
          val windowProperties = w.getWindowProperties
            .filter(!_.property.isInstanceOf[ProctimeAttribute])
            .map(_.name)
          // we have only a unique key if at least one window property is selected
          if (windowProperties.nonEmpty) {
            val windowId = windowProperties.min
            Some(groupKeys.map(e => (e, e)) ++ windowProperties.map(e => (e, windowId)))
          } else {
            None
          }

        case j: DataStreamJoin =>
          // get key(s) for join
          val lInKeys = visit(j.getLeft)
          val rInKeys = visit(j.getRight)
          if (lInKeys.isEmpty || rInKeys.isEmpty) {
            None
          } else {
            // Output of join must have keys if left and right both contain key(s).
            // Key groups from both side will be merged by join equi-predicates
            val lInNames: Seq[String] = j.getLeft.getRowType.getFieldNames
            val rInNames: Seq[String] = j.getRight.getRowType.getFieldNames
            val joinNames = j.getRowType.getFieldNames

            // if right field names equal to left field names, calcite will rename right
            // field names. For example, T1(pk, a) join T2(pk, b), calcite will rename T2(pk, b)
            // to T2(pk0, b).
            val rInNamesToJoinNamesMap = rInNames
              .zip(joinNames.subList(lInNames.size, joinNames.length))
              .toMap

            val lJoinKeys: Seq[String] = j.getJoinInfo.leftKeys
              .map(lInNames.get(_))
            val rJoinKeys: Seq[String] = j.getJoinInfo.rightKeys
              .map(rInNames.get(_))
              .map(rInNamesToJoinNamesMap(_))

            val inKeys: Seq[(String, String)] = lInKeys.get ++ rInKeys.get
              .map(e => (rInNamesToJoinNamesMap(e._1), rInNamesToJoinNamesMap(e._2)))

            getOutputKeysForNonWindowJoin(
              joinNames,
              inKeys,
              lJoinKeys.zip(rJoinKeys)
            )
          }
        case _: DataStreamRel =>
          // anything else does not forward keys, so we can stop
          None
      }
    }

    /**
      * Get output keys for non-window join according to it's inputs.
      *
      * @param inNames  Field names of join
      * @param inKeys   Input keys of join
      * @param joinKeys JoinKeys of join
      * @return Return output keys of join
      */
    def getOutputKeysForNonWindowJoin(
        inNames: Seq[String],
        inKeys: Seq[(String, String)],
        joinKeys: Seq[(String, String)])
    : Option[Seq[(String, String)]] = {

      val nameToGroups = mutable.HashMap.empty[String,String]

      // merge two groups
      def merge(nameA: String, nameB: String): Unit = {
        val ga: String = findGroup(nameA)
        val gb: String = findGroup(nameB)
        if (!ga.equals(gb)) {
          if (ga.compare(gb) < 0) {
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
      inNames.foreach(findGroup)

      val outputGroups = inKeys.map(e => nameToGroups(e._1)).distinct
      Some(
        inNames
          .filter(e => outputGroups.contains(nameToGroups(e)))
          .map(e => (e, nameToGroups(e)))
      )
    }
  }
}
