/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.scala.analysis

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts._
import eu.stratosphere.compiler.dag._
import eu.stratosphere.api.operators.Operator
import eu.stratosphere.api.operators.GenericDataSink
import eu.stratosphere.api.operators.DualInputOperator
import eu.stratosphere.api.operators.SingleInputOperator
import eu.stratosphere.api.record.operators.MapOperator
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.api.operators.GenericDataSource
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.api.record.operators.ReduceOperator
import eu.stratosphere.api.record.operators.CrossOperator
import eu.stratosphere.scala.TwoInputScalaContract
import eu.stratosphere.api.record.operators.JoinOperator
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.api.record.operators.CoGroupOperator
import eu.stratosphere.api.operators.WorksetIteration
import eu.stratosphere.api.operators.BulkIteration
import eu.stratosphere.scala.WorksetIterationScalaContract
import eu.stratosphere.scala.BulkIterationScalaContract
import eu.stratosphere.scala.UnionScalaContract

object Extractors {

  object DataSinkNode {
    def unapply(node: Operator): Option[(UDF1[_, _], List[Operator])] = node match {
      case contract: GenericDataSink with ScalaContract[_] => {
        Some((contract.getUDF.asInstanceOf[UDF1[_, _]], node.asInstanceOf[GenericDataSink].getInputs().toList))
      }
      case _                               => None
    }
  }

  object DataSourceNode {
    def unapply(node: Operator): Option[(UDF0[_])] = node match {
      case contract: GenericDataSource[_] with ScalaContract[_] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
      case _                                 => None
    }
  }

  object CoGroupNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, List[Operator], List[Operator])] = node match {
      case contract: CoGroupOperator with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputOperator[_]].getSecondInputs().toList))
      case _                                       => None
    }
  }

  object CrossNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], List[Operator], List[Operator])] = node match {
      case contract: CrossOperator with TwoInputScalaContract[_, _, _] => Some((contract.getUDF, contract.asInstanceOf[DualInputOperator[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputOperator[_]].getSecondInputs().toList))
      case _                                  => None
    }
  }

  object JoinNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, List[Operator], List[Operator])] = node match {
      case contract: JoinOperator with TwoInputKeyedScalaContract[ _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputOperator[_]].getSecondInputs().toList))
      case _                                    => None
    }
  }

  object MapNode {
    def unapply(node: Operator): Option[(UDF1[_, _], List[Operator])] = node match {
      case contract: MapOperator with OneInputScalaContract[_, _] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_]].getInputs().toList))
      case _                             => None
    }
  }
  
  object UnionNode {
    def unapply(node: Operator): Option[(UDF1[_, _], List[Operator])] = node match {
      case contract: MapOperator with UnionScalaContract[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_]].getInputs().toList))
      case _                             => None
    }
  }

  object ReduceNode {
    def unapply(node: Operator): Option[(UDF1[_, _], FieldSelector, List[Operator])] = node match {
      case contract: ReduceOperator with OneInputKeyedScalaContract[_, _] => Some((contract.getUDF, contract.key, contract.asInstanceOf[SingleInputOperator[_]].getInputs().toList))
      case contract: ReduceOperator with OneInputScalaContract[_, _] => Some((contract.getUDF, new FieldSelector(contract.getUDF.inputUDT, Nil), contract.asInstanceOf[SingleInputOperator[_]].getInputs().toList))
      case _                                   => None
    }
  }
 object WorksetIterationNode {
    def unapply(node: Operator): Option[(UDF0[_], FieldSelector, List[Operator], List[Operator])] = node match {
        case contract: WorksetIteration with WorksetIterationScalaContract[_] => Some((contract.getUDF, contract.key, contract.asInstanceOf[DualInputOperator[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputOperator[_]].getSecondInputs().toList))
        case _                                  => None
      }
  }
  
  object BulkIterationNode {
    def unapply(node: Operator): Option[(UDF0[_], List[Operator])] = node match {
      case contract: BulkIteration with BulkIterationScalaContract[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_]].getInputs().toList))
      case _ => None
    }
  } 
}
