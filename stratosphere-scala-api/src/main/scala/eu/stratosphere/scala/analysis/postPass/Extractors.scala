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

import eu.stratosphere.api.record.operators.CoGroupOperator
import eu.stratosphere.api.record.operators.CrossOperator
import eu.stratosphere.api.operators.GenericDataSink
import eu.stratosphere.api.operators.GenericDataSource
import eu.stratosphere.api.record.operators.MapOperator
import eu.stratosphere.api.record.operators.JoinOperator
import eu.stratosphere.api.record.operators.ReduceOperator
import eu.stratosphere.compiler.dag.BinaryUnionNode
import eu.stratosphere.compiler.dag.BulkIterationNode
import eu.stratosphere.compiler.dag.CoGroupNode
import eu.stratosphere.compiler.dag.CrossNode
import eu.stratosphere.compiler.dag.DataSinkNode
import eu.stratosphere.compiler.dag.DataSourceNode
import eu.stratosphere.compiler.dag.MapNode
import eu.stratosphere.compiler.dag.MatchNode
import eu.stratosphere.compiler.dag.OptimizerNode
import eu.stratosphere.compiler.dag.PactConnection
import eu.stratosphere.compiler.dag.ReduceNode
import eu.stratosphere.compiler.dag.SinkJoiner
import eu.stratosphere.compiler.dag.WorksetIterationNode
import eu.stratosphere.api.operators.WorksetIteration
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.scala.TwoInputScalaContract
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.scala.WorksetIterationScalaContract
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.analysis.UDF
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDF2
import eu.stratosphere.scala.BulkIterationScalaContract
import eu.stratosphere.api.operators.BulkIteration
import eu.stratosphere.scala.UnionScalaContract

object Extractors {

  implicit def nodeToGetUDF(node: OptimizerNode) = new {
    def getUDF: Option[UDF[_]] = node match {
      case _: SinkJoiner | _: BinaryUnionNode => None
      case _ => {
        Some(node.getPactContract.asInstanceOf[ScalaContract[_]].getUDF)
      }
    }
  }

  object DataSinkNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: DataSinkNode => node.getPactContract match {
        case contract: GenericDataSink with OneInputScalaContract[_, _] => {
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
        case contract: GenericDataSource[_] with ScalaContract[_] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
        case _ => None
      }
      case _ => None
    }
  }

  object CoGroupNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, PactConnection, PactConnection)] = node match {
      case node: CoGroupNode => node.getPactContract() match {
        case contract: CoGroupOperator with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object CrossNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: CrossNode => node.getPactContract match {
        case contract: CrossOperator with TwoInputScalaContract[_, _, _] => Some((contract.getUDF, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object JoinNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, PactConnection, PactConnection)] = node match {
      case node: MatchNode => node.getPactContract match {
        case contract: JoinOperator with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object MapNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: MapOperator with OneInputScalaContract[_, _] => Some((contract.getUDF, node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }
  
  object UnionNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: MapOperator with UnionScalaContract[_] => Some((contract.getUDF, node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object ReduceNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], FieldSelector, PactConnection)] = node match {
      case node: ReduceNode => node.getPactContract match {
        case contract: ReduceOperator with OneInputKeyedScalaContract[_, _] => Some((contract.getUDF, contract.key, node.getIncomingConnection))
        case contract: ReduceOperator with OneInputScalaContract[_, _] => Some((contract.getUDF, new FieldSelector(contract.getUDF.inputUDT, Nil), node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }
  object WorksetIterationNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_], FieldSelector, PactConnection, PactConnection)] = node match {
      case node: WorksetIterationNode => node.getPactContract match {
        case contract: WorksetIteration with WorksetIterationScalaContract[_] => Some((contract.getUDF, contract.key, node.getFirstIncomingConnection(), node.getSecondIncomingConnection()))
        case _                                  => None
      }
      case _ => None
    }
  }
  
  object BulkIterationNode {
    def unapply(node: OptimizerNode): Option[(UDF0[_], PactConnection)] = node match {
      case node: BulkIterationNode => node.getPactContract match {
        case contract: BulkIteration with BulkIterationScalaContract[_] => Some((contract.getUDF, node.getIncomingConnection()))
        case _                                  => None
      }
      case _ => None
    }
  }

}
