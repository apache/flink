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
//import scala.collection.mutable
//import scala.collection.JavaConversions._
//
//import eu.stratosphere.api.scala.analysis._
//import eu.stratosphere.api.scala.contracts._
//
//import eu.stratosphere.pact.compiler.plan._
//import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan
//
//object AmbientFieldDetector {
//
//  import Extractors._
//  import EdgeDependencySets.EdgeDependencySet
//
//  def updateAmbientFields(plan: OptimizedPlan, edgeDependencies: Map[PactConnection, EdgeDependencySet], outputPositions: Map[Int, GlobalPos]): Unit = {
//    plan.getDataSinks.map(_.getSinkNode).foldLeft(Set[OptimizerNode]())(updateAmbientFields(outputPositions, edgeDependencies))
//  }
//
//  private def updateAmbientFields(outputPositions: Map[Int, GlobalPos], edgeDependencies: Map[PactConnection, EdgeDependencySet])(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {
//
//    visited.contains(node) match {
//
//      case true => visited
//
//      case false => {
//        node match {
//
//          case _: SinkJoiner | _: BinaryUnionNode =>
//          case DataSinkNode(udf, input)                       =>
//          case DataSourceNode(udf)                            =>
//
//          case CoGroupNode(udf, _, _, leftInput, rightInput) => {
//
//            val leftProvides = edgeDependencies(leftInput).childProvides
//            val rightProvides = edgeDependencies(rightInput).childProvides
//            val parentNeeds = edgeDependencies(node.getOutgoingConnections.head).childProvides
//            val writes = udf.outputFields.toIndexSet
//
//            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
//            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
//          }
//
//          case CrossNode(udf, leftInput, rightInput) => {
//
//            val leftProvides = edgeDependencies(leftInput).childProvides
//            val rightProvides = edgeDependencies(rightInput).childProvides
//            val parentNeeds = edgeDependencies(node.getOutgoingConnections.head).childProvides
//            val writes = udf.outputFields.toIndexSet
//
//            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
//            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
//          }
//
//          case JoinNode(udf, _, _, leftInput, rightInput) => {
//
//            val leftProvides = edgeDependencies(leftInput).childProvides
//            val rightProvides = edgeDependencies(rightInput).childProvides
//            val parentNeeds = edgeDependencies(node.getOutgoingConnections.head).childProvides
//            val writes = udf.outputFields.toIndexSet
//
//            populateSets(udf.leftForwardSet, udf.leftDiscardSet, leftProvides, parentNeeds, writes, outputPositions)
//            populateSets(udf.rightForwardSet, udf.rightDiscardSet, rightProvides, parentNeeds, writes, outputPositions)
//          }
//
//          case MapNode(udf, input) => {
//
//            val inputProvides = edgeDependencies(input).childProvides
//            val parentNeeds = edgeDependencies(node.getOutgoingConnections.head).childProvides
//            val writes = udf.outputFields.toIndexSet
//
//            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
//          }
//
//          case ReduceNode(udf, _, input) => {
//            val inputProvides = edgeDependencies(input).childProvides
//            val parentNeeds = edgeDependencies(node.getOutgoingConnections.head).childProvides
//            val writes = udf.outputFields.toIndexSet
//
//            populateSets(udf.forwardSet, udf.discardSet, inputProvides, parentNeeds, writes, outputPositions)
//          }
//        }
//
//        node.getIncomingConnections.map(_.getSource).foldLeft(visited + node)(updateAmbientFields(outputPositions, edgeDependencies))
//      }
//    }
//  }
//
//  private def populateSets(forwards: mutable.Set[GlobalPos], discards: mutable.Set[GlobalPos], childProvides: Set[Int], parentNeeds: Set[Int], writes: Set[Int], outputPositions: Map[Int, GlobalPos]): Unit = {
//    forwards.clear()
//    forwards.addAll((parentNeeds -- writes).intersect(childProvides).map(outputPositions(_)))
//
//    discards.clear()
//    discards.addAll((childProvides -- parentNeeds -- writes).intersect(childProvides).map(outputPositions(_)))
//  }
//}
