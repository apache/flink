// Comment out because this is not working right now
///**
// * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package eu.stratosphere.api.scala.analysis.postPass
//
//import scala.collection.JavaConversions._
//
//import eu.stratosphere.api.scala.analysis._
//import eu.stratosphere.api.scala.contracts._
//
//import eu.stratosphere.pact.compiler.plan._
//import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan
//
//object EdgeDependencySets {
//
//  import Extractors._
//
//  case class EdgeDependencySet(parentNeeds: Set[Int], childProvides: Set[Int] = Set())
//
//  def computeEdgeDependencySets(plan: OptimizedPlan, outputSets: Map[OptimizerNode, Set[Int]]): Map[PactConnection, EdgeDependencySet] = {
//
//    plan.getDataSinks.map(_.getSinkNode).foldLeft(Map[PactConnection, EdgeDependencySet]())(computeEdgeDependencySets(outputSets))
//  }
//
//  private def computeEdgeDependencySets(outputSets: Map[OptimizerNode, Set[Int]])(edgeDependencySets: Map[PactConnection, EdgeDependencySet], node: OptimizerNode): Map[PactConnection, EdgeDependencySet] = {
//
//    // breadth-first traversal: parentNeeds will be None if any parent has not yet been visited
//    val parentNeeds = node.getOutgoingConnections().foldLeft(Option(Set[Int]())) {
//      case (None, _)           => None
//      case (Some(acc), parent) => edgeDependencySets.get(parent) map { acc ++ _.parentNeeds }
//    }
//
//    parentNeeds match {
//      case None              => edgeDependencySets
//      case Some(parentNeeds) => computeEdgeDependencySets(node, parentNeeds, outputSets, edgeDependencySets)
//    }
//  }
//
//  private def computeEdgeDependencySets(node: OptimizerNode, parentNeeds: Set[Int], outputSets: Map[OptimizerNode, Set[Int]], edgeDependencySets: Map[PactConnection, EdgeDependencySet]): Map[PactConnection, EdgeDependencySet] = {
//
//    def updateEdges(needs: (PactConnection, Set[Int])*): Map[PactConnection, EdgeDependencySet] = {
//
//      val updParents = node.getOutgoingConnections().foldLeft(edgeDependencySets) { (edgeDependencySets, parent) =>
//        val entry = edgeDependencySets(parent)
//        edgeDependencySets.updated(parent, entry.copy(childProvides = parentNeeds))
//      }
//
//      needs.foldLeft(updParents) {
//        case (edgeDependencySets, (inConn, needs)) => {
//          val updInConn = edgeDependencySets.updated(inConn, EdgeDependencySet(needs))
//          computeEdgeDependencySets(outputSets)(updInConn, inConn.getSource)
//        }
//      }
//    }
//
//    for (udf <- node.getUDF) {
//
//      // suppress outputs that aren't needed by any parent
//      val writeFields = udf.outputFields filter { _.isUsed }
//      val unused = writeFields filterNot { f => parentNeeds.contains(f.globalPos.getValue) }
//
//      for (field <- unused) {
//        field.isUsed = false
//        if (field.globalPos.isIndex)
//          field.globalPos.setIndex(Int.MinValue)
//      }
//    }
//
//    node match {
//
//      case DataSinkNode(udf, input) => {
//        val needs = udf.inputFields.toIndexSet
//        updateEdges(input -> needs)
//      }
//
//      case DataSourceNode(udf) => {
//        updateEdges()
//      }
//
//      case CoGroupNode(udf, leftKey, rightKey, leftInput, rightInput) => {
//
//        val leftReads = udf.leftInputFields.toIndexSet ++ leftKey.selectedFields.toIndexSet
//        val rightReads = udf.rightInputFields.toIndexSet ++ rightKey.selectedFields.toIndexSet
//        val writes = udf.outputFields.toIndexSet
//
//        val parentPreNeeds = parentNeeds -- writes
//
//        val parentLeftNeeds = parentPreNeeds.intersect(outputSets(leftInput.getSource))
//        val parentRightNeeds = parentPreNeeds.intersect(outputSets(rightInput.getSource))
//
//        val leftForwards = udf.leftForwardSet.map(_.getValue)
//        val rightForwards = udf.rightForwardSet.map(_.getValue)
//
//        val (leftRes, rightRes) = parentLeftNeeds.intersect(parentRightNeeds).partition {
//          case index if leftForwards(index) && !rightForwards(index) => true
//          case index if !leftForwards(index) && rightForwards(index) => false
//          case _                                                     => throw new UnsupportedOperationException("Schema conflict: cannot forward the same field from both sides of a two-input operator.")
//        }
//
//        val leftNeeds = (parentLeftNeeds -- rightRes) ++ leftReads
//        val rightNeeds = (parentRightNeeds -- leftRes) ++ rightReads
//
//        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
//      }
//
//      case CrossNode(udf, leftInput, rightInput) => {
//
//        val leftReads = udf.leftInputFields.toIndexSet
//        val rightReads = udf.rightInputFields.toIndexSet
//        val writes = udf.outputFields.toIndexSet
//
//        val parentPreNeeds = parentNeeds -- writes
//
//        val parentLeftNeeds = parentPreNeeds.intersect(outputSets(leftInput.getSource))
//        val parentRightNeeds = parentPreNeeds.intersect(outputSets(rightInput.getSource))
//
//        val leftForwards = udf.leftForwardSet.map(_.getValue)
//        val rightForwards = udf.rightForwardSet.map(_.getValue)
//
//        val (leftRes, rightRes) = parentLeftNeeds.intersect(parentRightNeeds).partition {
//          case index if leftForwards(index) && !rightForwards(index) => true
//          case index if !leftForwards(index) && rightForwards(index) => false
//          case _                                                     => throw new UnsupportedOperationException("Schema conflict: cannot forward the same field from both sides of a two-input operator.")
//        }
//
//        val leftNeeds = (parentLeftNeeds -- rightRes) ++ leftReads
//        val rightNeeds = (parentRightNeeds -- leftRes) ++ rightReads
//
//        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
//      }
//
//      case JoinNode(udf, leftKey, rightKey, leftInput, rightInput) => {
//
//        val leftReads = udf.leftInputFields.toIndexSet ++ leftKey.selectedFields.toIndexSet
//        val rightReads = udf.rightInputFields.toIndexSet ++ rightKey.selectedFields.toIndexSet
//        val writes = udf.outputFields.toIndexSet
//
//        val parentPreNeeds = parentNeeds -- writes
//
//        val parentLeftNeeds = parentPreNeeds.intersect(outputSets(leftInput.getSource))
//        val parentRightNeeds = parentPreNeeds.intersect(outputSets(rightInput.getSource))
//
//        val leftForwards = udf.leftForwardSet.map(_.getValue)
//        val rightForwards = udf.rightForwardSet.map(_.getValue)
//
//        val (leftRes, rightRes) = parentLeftNeeds.intersect(parentRightNeeds).partition {
//          case index if leftForwards(index) && !rightForwards(index) => true
//          case index if !leftForwards(index) && rightForwards(index) => false
//          case _                                                     => throw new UnsupportedOperationException("Schema conflict: cannot forward the same field from both sides of a two-input operator.")
//        }
//
//        val leftNeeds = (parentLeftNeeds -- rightRes) ++ leftReads
//        val rightNeeds = (parentRightNeeds -- leftRes) ++ rightReads
//
//        updateEdges(leftInput -> leftNeeds, rightInput -> rightNeeds)
//      }
//
//      case MapNode(udf, input) => {
//
//        val reads = udf.inputFields.toIndexSet
//        val writes = udf.outputFields.toIndexSet
//
//        val needs = parentNeeds -- writes ++ reads
//
//        updateEdges(input -> needs)
//      }
//
//      case ReduceNode(udf, key, input) => {
//
//        val reads = udf.inputFields.toIndexSet ++ key.selectedFields.toIndexSet
//        val writes = udf.outputFields.toIndexSet
//
//        val needs = parentNeeds -- writes ++ reads
//
//        updateEdges(input -> needs)
//      }
//
//      case _: SinkJoiner | _: BinaryUnionNode => {
//        updateEdges(node.getIncomingConnections.map(_ -> parentNeeds): _*)
//      }
//    }
//  }
//}
