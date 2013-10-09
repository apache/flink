/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.analysis.postPass

import scala.collection.mutable
import scala.collection.JavaConversions._

import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts._

import eu.stratosphere.pact.compiler.plan._
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan

object GlobalSchemaCompactor {

  import Extractors._

  def compactSchema(plan: OptimizedPlan): Unit = {

    val (_, conflicts) = plan.getDataSinks.map(_.getSinkNode).foldLeft((Set[OptimizerNode](), Map[GlobalPos, Set[GlobalPos]]())) {
      case ((visited, conflicts), node) => findConflicts(node, visited, conflicts)
    }

    // Reset all position indexes before reassigning them 
    conflicts.keys.foreach { _.setIndex(Int.MinValue) }

    plan.getDataSinks.map(_.getSinkNode).foldLeft(Set[OptimizerNode]())(compactSchema(conflicts))
  }

  /**
   * Two fields are in conflict when they exist in the same place (record) at the same time (plan node).
   * If two fields are in conflict, then they must be assigned different indexes.
   *
   * p1 conflictsWith p2 =
   *   Exists(n in Nodes):
   *     p1 != p2 &&
   *     (
   *       (p1 in n.forwards && p2 in n.forwards) ||
   *       (p1 in n.forwards && p2 in n.outputs) ||
   *       (p2 in n.forwards && p1 in n.outputs)
   *     )
   */
  private def findConflicts(node: OptimizerNode, visited: Set[OptimizerNode], conflicts: Map[GlobalPos, Set[GlobalPos]]): (Set[OptimizerNode], Map[GlobalPos, Set[GlobalPos]]) = {

    visited.contains(node) match {

      case true => (visited, conflicts)

      case false => {

        val (forwardPos, outputFields) = node.getUDF match {
          case None                     => (Set[GlobalPos](), Set[OutputField]())
          case Some(udf: UDF0[_])       => (Set[GlobalPos](), udf.outputFields.toSet)
          case Some(udf: UDF1[_, _])    => (udf.forwardSet, udf.outputFields.toSet)
          case Some(udf: UDF2[_, _, _]) => (udf.leftForwardSet ++ udf.rightForwardSet, udf.outputFields.toSet)
          case _        => (Set[GlobalPos](), Set[OutputField]())
        }

        // resolve GlobalPos references to the instance that holds the actual index 
        val forwards = forwardPos map { _.resolve }
        val outputs = outputFields filter { _.isUsed } map { _.globalPos.resolve }

        val newConflictsF = forwards.foldLeft(conflicts) {
          case (conflicts, fPos) => {
            // add all other forwards and all outputs to this forward's conflict set
            val fConflicts = conflicts.getOrElse(fPos, Set()) ++ (forwards filterNot { _ == fPos }) ++ outputs
            conflicts.updated(fPos, fConflicts)
          }
        }

        val newConflictsO = outputs.foldLeft(newConflictsF) {
          case (conflicts, oPos) => {
            // add all forwards to this output's conflict set
            val oConflicts = conflicts.getOrElse(oPos, Set()) ++ forwards
            conflicts.updated(oPos, oConflicts)
          }
        }

        node.getIncomingConnections.map(_.getSource).foldLeft((visited + node, newConflictsO)) {
          case ((visited, conflicts), node) => findConflicts(node, visited, conflicts)
        }
      }
    }
  }

  /**
   * Assign indexes bottom-up, giving lower values to fields with larger conflict sets.
   * This ordering should do a decent job of minimizing the number of gaps between fields.
   */
  private def compactSchema(conflicts: Map[GlobalPos, Set[GlobalPos]])(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {

        val newVisited = node.getIncomingConnections.map(_.getSource).foldLeft(visited + node)(compactSchema(conflicts))

        val outputFields = node.getUDF match {
          case None      => Seq[OutputField]()
          case Some(udf) => udf.outputFields filter { _.isUsed }
        }

        val outputs = outputFields map {
          case field => {
            val pos = field.globalPos.resolve
            (pos, field.localPos, conflicts(pos) map { _.getValue })
          }
        } sortBy {
          case (_, localPos, posConflicts) => (Int.MaxValue - posConflicts.size, localPos)
        }

        val initUsed = outputs map { _._1.getValue } filter { _ >= 0 } toSet

        val used = outputs.filter(_._1.getValue < 0).foldLeft(initUsed) {
          case (used, (pos, _, conflicts)) => {
            val index = chooseIndexValue(used ++ conflicts)
            pos.setIndex(index)
            used + index
          }
        }

        node.getUDF match {
          case Some(udf: UDF1[_, _])    => updateDiscards(used, udf.discardSet)
          case Some(udf: UDF2[_, _, _]) => updateDiscards(used, udf.leftDiscardSet, udf.rightDiscardSet)
          case _                        =>
        }

        newVisited
      }
    }
  }

  private def chooseIndexValue(used: Set[Int]): Int = {
    var index = 0
    while (used(index)) {
      index = index + 1
    }
    index
  }

  private def updateDiscards(outputs: Set[Int], discardSets: mutable.Set[GlobalPos]*): Unit = {
    for (discardSet <- discardSets) {

      val overwrites = discardSet filter { pos => outputs.contains(pos.getValue) } toList

      for (pos <- overwrites)
        discardSet.remove(pos)
    }
  }
}
