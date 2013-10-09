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

import eu.stratosphere.pact.common.contract.CoGroupContract
import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.pact.common.contract.GenericDataSink
import eu.stratosphere.pact.common.contract.GenericDataSource
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.contract.MatchContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.compiler.plan.BinaryUnionNode
import eu.stratosphere.pact.compiler.plan.BulkIterationNode
import eu.stratosphere.pact.compiler.plan.CoGroupNode
import eu.stratosphere.pact.compiler.plan.CrossNode
import eu.stratosphere.pact.compiler.plan.DataSinkNode
import eu.stratosphere.pact.compiler.plan.DataSourceNode
import eu.stratosphere.pact.compiler.plan.MapNode
import eu.stratosphere.pact.compiler.plan.MatchNode
import eu.stratosphere.pact.compiler.plan.OptimizerNode
import eu.stratosphere.pact.compiler.plan.PactConnection
import eu.stratosphere.pact.compiler.plan.ReduceNode
import eu.stratosphere.pact.compiler.plan.SinkJoiner
import eu.stratosphere.pact.compiler.plan.WorksetIterationNode
import eu.stratosphere.pact.generic.contract.WorksetIteration
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
import eu.stratosphere.pact.generic.contract.BulkIteration
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
        case contract: CoGroupContract with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object CrossNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], PactConnection, PactConnection)] = node match {
      case node: CrossNode => node.getPactContract match {
        case contract: CrossContract with TwoInputScalaContract[_, _, _] => Some((contract.getUDF, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object JoinNode {
    def unapply(node: OptimizerNode): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, PactConnection, PactConnection)] = node match {
      case node: MatchNode => node.getPactContract match {
        case contract: MatchContract with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, node.getFirstIncomingConnection, node.getSecondIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object MapNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: MapContract with OneInputScalaContract[_, _] => Some((contract.getUDF, node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }
  
  object UnionNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], PactConnection)] = node match {
      case node: MapNode => node.getPactContract match {
        case contract: MapContract with UnionScalaContract[_] => Some((contract.getUDF, node.getIncomingConnection))
        case _ => None
      }
      case _ => None
    }
  }

  object ReduceNode {
    def unapply(node: OptimizerNode): Option[(UDF1[_, _], FieldSelector, PactConnection)] = node match {
      case node: ReduceNode => node.getPactContract match {
        case contract: ReduceContract with OneInputKeyedScalaContract[_, _] => Some((contract.getUDF, contract.key, node.getIncomingConnection))
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
