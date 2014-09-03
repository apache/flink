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

import scala.language.implicitConversions

import scala.Some

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.analysis.FieldSelector
import org.apache.flink.api.scala.analysis.UDF
import org.apache.flink.api.scala.analysis.UDF0
import org.apache.flink.api.scala.analysis.UDF1
import org.apache.flink.api.scala.analysis.UDF2

import org.apache.flink.api.java.record.operators.CoGroupOperator
import org.apache.flink.api.java.record.operators.CrossOperator
import org.apache.flink.api.java.record.operators.MapOperator
import org.apache.flink.api.java.record.operators.JoinOperator
import org.apache.flink.api.java.record.operators.ReduceOperator
import org.apache.flink.compiler.dag.BinaryUnionNode
import org.apache.flink.compiler.dag.BulkIterationNode
import org.apache.flink.compiler.dag.CoGroupNode
import org.apache.flink.compiler.dag.CrossNode
import org.apache.flink.compiler.dag.DataSinkNode
import org.apache.flink.compiler.dag.DataSourceNode
import org.apache.flink.compiler.dag.CollectorMapNode
import org.apache.flink.compiler.dag.MatchNode
import org.apache.flink.compiler.dag.OptimizerNode
import org.apache.flink.compiler.dag.PactConnection
import org.apache.flink.compiler.dag.GroupReduceNode
import org.apache.flink.compiler.dag.SinkJoiner
import org.apache.flink.compiler.dag.WorksetIterationNode
import org.apache.flink.api.common.operators.Union
import org.apache.flink.api.common.operators.base.{BulkIterationBase => BulkIteration, DeltaIterationBase => DeltaIteration, GenericDataSinkBase, GenericDataSourceBase}

object Extractors {

  implicit def nodeToGetUDF(node: OptimizerNode) = new {
    def getUDF: Option[UDF[_]] = node match {
      case _: SinkJoiner | _: BinaryUnionNode => None
      case _ => {
        Some(node.getPactContract.asInstanceOf[ScalaOperator[_, _]].getUDF)
      }
    }
  }

  object DataSinkNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: DataSinkNode => node.getPactContract match {
        case contract: GenericDataSinkBase[_] with ScalaOutputOperator[_] => {
          Some((contract.getUDF, node.getInputConnection))
        }
        case _  => None
      }
      case _ => None
    }
  }

  object DataSourceNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_])] = node match {
      case node: DataSourceNode => node.getPactContract() match {
        case contract: GenericDataSourceBase[_, _] with ScalaOperator[_, _] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
        case _ => None
      }
      case _ => None
    }
  }

  object CoGroupNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, PactConnection, PactConnection)] = node match {
      case node: CoGroupNode => node.getPactContract() match {
        case contract: CoGroupOperator with TwoInputKeyedScalaOperator[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object CrossNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: CrossNode => node.getPactContract match {
        case contract: CrossOperator with TwoInputScalaOperator[_, _, _] => Some((contract.getUDF, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object JoinNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, PactConnection, PactConnection)] = node match {
      case node: MatchNode => node.getPactContract match {
        case contract: JoinOperator with TwoInputKeyedScalaOperator[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object MapNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: CollectorMapNode => node.getPactContract match {
        case contract: MapOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }
  
  object UnionNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: BinaryUnionNode => node.getPactContract match {
        case contract: Union[_] with UnionScalaOperator[_] => Some((contract.getUDF, node.getFirstIncomingConnection(), node.getSecondIncomingConnection()))
        case _ => None
      }
      case _ => None
    }
  }

  object ReduceNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], FieldSelector, PactConnection)] = node match {
      case node: GroupReduceNode => node.getPactContract match {
        case contract: ReduceOperator with OneInputKeyedScalaOperator[_, _] => Some((contract.getUDF, contract.key, node.getIncomingConnection))
        case contract: ReduceOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, new FieldSelector(contract.getUDF.inputUDT, Nil), node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }
  object DeltaIterationNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_], FieldSelector, PactConnection, PactConnection)] = node match {
      case node: WorksetIterationNode => node.getPactContract match {
        case contract: DeltaIteration[_, _] with DeltaIterationScalaOperator[_] => Some((contract.getUDF, contract.key, node.getFirstIncomingConnection(), node.getSecondIncomingConnection()))
        case _                                  => None
      }
      case _ => None
    }
  }
  
  object BulkIterationNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_], PactConnection)] = node match {
      case node: BulkIterationNode => node.getPactContract match {
        case contract: BulkIteration[_] with BulkIterationScalaOperator[_] => Some((contract.getUDF, node.getIncomingConnection()))
        case _                                  => None
      }
      case _ => None
    }
  }

}
