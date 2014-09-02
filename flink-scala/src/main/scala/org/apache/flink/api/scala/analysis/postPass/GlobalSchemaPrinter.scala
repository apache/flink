/**
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


package org.apache.flink.api.scala.analysis.postPass

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable

import org.apache.commons.logging.{LogFactory, Log}

import Extractors.CoGroupNode
import Extractors.CrossNode
import Extractors.DataSinkNode
import Extractors.DataSourceNode
import Extractors.JoinNode
import Extractors.MapNode
import Extractors.ReduceNode

import org.apache.flink.api.scala.analysis.FieldSet
import org.apache.flink.api.scala.analysis.FieldSelector

import org.apache.flink.compiler.dag.BinaryUnionNode
import org.apache.flink.compiler.dag.OptimizerNode
import org.apache.flink.compiler.dag.SinkJoiner
import org.apache.flink.compiler.plan.OptimizedPlan
import org.apache.flink.api.common.Plan
import org.apache.flink.api.common.operators.Operator
import org.apache.flink.api.common.operators.SingleInputOperator
import org.apache.flink.api.common.operators.DualInputOperator
import org.apache.flink.api.java.record.operators.GenericDataSink

object GlobalSchemaPrinter {

  import Extractors._

  private final val LOG: Log = LogFactory.getLog(classOf[GlobalSchemaOptimizer])

  def printSchema(plan: OptimizedPlan): Unit = {

    LOG.debug("### " + plan.getJobName + " ###")
    plan.getDataSinks.map(_.getSinkNode).foldLeft(Set[OptimizerNode]())(printSchema)
    LOG.debug("####" + ("#" * plan.getJobName.length) + "####")
  }

  private def printSchema(visited: Set[OptimizerNode], node: OptimizerNode): Set[OptimizerNode] = {

    visited.contains(node) match {

      case true => visited

      case false => {

        val children = node.getIncomingConnections.map(_.getSource).toSet
        val newVisited = children.foldLeft(visited + node)(printSchema)

        node match {

          case _: SinkJoiner | _: BinaryUnionNode =>

          case DataSinkNode(udf, input) => {
            printInfo(node, "Sink",
              Seq(),
              Seq(("", udf.inputFields)),
              Seq(("", udf.getForwardIndexArrayFrom)),
              Seq(("", udf.getDiscardIndexArray)),
              udf.outputFields
            )
          }

          case DataSourceNode(udf) => {
            printInfo(node, "Source",
              Seq(),
              Seq(),
              Seq(),
              Seq(),
              udf.outputFields
            )
          }

          case CoGroupNode(udf, leftKey, rightKey, leftInput, rightInput) => {
            printInfo(node, "CoGroup",
              Seq(("L", leftKey), ("R", rightKey)),
              Seq(("L", udf.leftInputFields), ("R", udf.rightInputFields)),
              Seq(("L", udf.getLeftForwardIndexArrayFrom), ("R", udf.getRightForwardIndexArrayFrom)),
              Seq(("L", udf.getLeftDiscardIndexArray), ("R", udf.getRightDiscardIndexArray)),
              udf.outputFields
            )
          }

          case CrossNode(udf, leftInput, rightInput) => {
            printInfo(node, "Cross",
              Seq(),
              Seq(("L", udf.leftInputFields), ("R", udf.rightInputFields)),
              Seq(("L", udf.getLeftForwardIndexArrayFrom), ("R", udf.getRightForwardIndexArrayFrom)),
              Seq(("L", udf.getLeftDiscardIndexArray), ("R", udf.getRightDiscardIndexArray)),
              udf.outputFields
            )
          }

          case JoinNode(udf, leftKey, rightKey, leftInput, rightInput) => {
            printInfo(node, "Join",
              Seq(("L", leftKey), ("R", rightKey)),
              Seq(("L", udf.leftInputFields), ("R", udf.rightInputFields)),
              Seq(("L", udf.getLeftForwardIndexArrayFrom), ("R", udf.getRightForwardIndexArrayFrom)),
              Seq(("L", udf.getLeftDiscardIndexArray), ("R", udf.getRightDiscardIndexArray)),
              udf.outputFields
            )
          }

          case MapNode(udf, input) => {
            printInfo(node, "Map",
              Seq(),
              Seq(("", udf.inputFields)),
              Seq(("", udf.getForwardIndexArrayFrom)),
              Seq(("", udf.getDiscardIndexArray)),
              udf.outputFields
            )
          }

          case UnionNode(udf, leftInput, rightInput) => {
            printInfo(node, "Union",
              Seq(),
              Seq(("L", udf.leftInputFields), ("R", udf.rightInputFields)),
              Seq(("L", udf.getLeftForwardIndexArrayFrom), ("R", udf.getRightForwardIndexArrayFrom)),
              Seq(("L", udf.getLeftDiscardIndexArray), ("R", udf.getRightDiscardIndexArray)),
              udf.outputFields
            )
          }

          case ReduceNode(udf, key, input) => {

//            val contract = node.asInstanceOf[Reduce4sContract[_, _, _]]
//            contract.userCombineCode map { _ =>
//              printInfo(node, "Combine",
//                Seq(("", key)),
//                Seq(("", udf.inputFields)),
//                Seq(("", contract.combineForwardSet.toArray)),
//                Seq(("", contract.combineDiscardSet.toArray)),
//                udf.inputFields
//              )
//            }

            printInfo(node, "Reduce",
              Seq(("", key)),
              Seq(("", udf.inputFields)),
              Seq(("", udf.getForwardIndexArrayFrom)),
              Seq(("", udf.getDiscardIndexArray)),
              udf.outputFields
            )
          }
          case DeltaIterationNode(udf, key, input1, input2) => {

            printInfo(node, "WorksetIterate",
              Seq(("", key)),
              Seq(),
              Seq(),
              Seq(),
              udf.outputFields)
          }

          case BulkIterationNode(udf, input1) => {

            printInfo(node, "BulkIterate",
              Seq(),
              Seq(),
              Seq(),
              Seq(),
              udf.outputFields)
          }

        }

        newVisited
      }
    }
  }

  private def printInfo(node: OptimizerNode, kind: String, keys: Seq[(String, FieldSelector)], reads: Seq[(String, FieldSet[_])], forwards: Seq[(String, Array[Int])], discards: Seq[(String, Array[Int])], writes: FieldSet[_]): Unit = {

    def indexesToStrings(pre: String, indexes: Array[Int]) = indexes map {
      case -1 => "_"
      case i  => pre + i
    }

    val formatString = "%s (%s): K{%s}: R[%s] => F[%s] - D[%s] + W[%s]"

    val name = node.getName

    val sKeys = keys flatMap { case (pre, value) => value.selectedFields.toSerializerIndexArray.map(pre + _) } mkString ", "
    val sReads = reads flatMap { case (pre, value) => indexesToStrings(pre, value.toSerializerIndexArray) } mkString ", "
    val sForwards = forwards flatMap { case (pre, value) => value.sorted.map(pre + _) } mkString ", "
    val sDiscards = discards flatMap { case (pre, value) => value.sorted.map(pre + _) } mkString ", "
    val sWrites = indexesToStrings("", writes.toSerializerIndexArray) mkString ", "

    LOG.debug(formatString.format(name, kind, sKeys, sReads, sForwards, sDiscards, sWrites))
  }
}
