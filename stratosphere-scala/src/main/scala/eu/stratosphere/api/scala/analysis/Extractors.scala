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

package eu.stratosphere.api.scala.analysis

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import eu.stratosphere.api.scala.analysis._
import eu.stratosphere.api.scala.operators.Annotations
import eu.stratosphere.compiler.dag._
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.common.operators.GenericDataSink
import eu.stratosphere.api.common.operators.DualInputOperator
import eu.stratosphere.api.common.operators.SingleInputOperator
import eu.stratosphere.api.java.record.operators.MapOperator
import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.common.operators.GenericDataSource
import eu.stratosphere.api.scala.OneInputScalaOperator
import eu.stratosphere.api.scala.OneInputKeyedScalaOperator
import eu.stratosphere.api.java.record.operators.ReduceOperator
import eu.stratosphere.api.java.record.operators.CrossOperator
import eu.stratosphere.api.scala.TwoInputScalaOperator
import eu.stratosphere.api.java.record.operators.JoinOperator
import eu.stratosphere.api.scala.TwoInputKeyedScalaOperator
import eu.stratosphere.api.java.record.operators.CoGroupOperator
import eu.stratosphere.api.common.operators.DeltaIteration
import eu.stratosphere.api.common.operators.BulkIteration
import eu.stratosphere.api.scala.DeltaIterationScalaOperator
import eu.stratosphere.api.scala.BulkIterationScalaOperator
import eu.stratosphere.api.scala.UnionScalaOperator
import eu.stratosphere.api.common.operators.Union

object Extractors {

  object DataSinkNode {
    def unapply(node: Operator): Option[(UDF1[_, _], Operator)] = node match {
      case contract: GenericDataSink with ScalaOperator[_] => {
        Some((contract.getUDF.asInstanceOf[UDF1[_, _]], node.asInstanceOf[GenericDataSink].getInput()))
      }
      case _                               => None
    }
  }

  object DataSourceNode {
    def unapply(node: Operator): Option[(UDF0[_])] = node match {
      case contract: GenericDataSource[_] with ScalaOperator[_] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
      case _                                 => None
    }
  }

  object CoGroupNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, Operator, Operator)] = node match {
      case contract: CoGroupOperator with TwoInputKeyedScalaOperator[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_]].getFirstInput(), contract.asInstanceOf[DualInputOperator[_]].getSecondInput()))
      case _                                       => None
    }
  }

  object CrossNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], Operator, Operator)] = node match {
      case contract: CrossOperator with TwoInputScalaOperator[_, _, _] => Some((contract.getUDF, contract.asInstanceOf[DualInputOperator[_]].getFirstInput(), contract.asInstanceOf[DualInputOperator[_]].getSecondInput()))
      case _                                  => None
    }
  }

  object JoinNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, Operator, Operator)] = node match {
      case contract: JoinOperator with TwoInputKeyedScalaOperator[ _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_]].getFirstInput(), contract.asInstanceOf[DualInputOperator[_]].getSecondInput()))
      case _                                    => None
    }
  }

  object MapNode {
    def unapply(node: Operator): Option[(UDF1[_, _], Operator)] = node match {
      case contract: MapOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_]].getInput()))
      case _                             => None
    }
  }
  
  object UnionNode {
    def unapply(node: Operator): Option[(UDF2[_, _, _], Operator, Operator)] = node match {
      case contract: Union with UnionScalaOperator[_] => Some((contract.getUDF, contract.asInstanceOf[DualInputOperator[_]].getFirstInput(), contract.asInstanceOf[DualInputOperator[_]].getSecondInput()))
      case _                             => None
    }
  }

  object ReduceNode {
    def unapply(node: Operator): Option[(UDF1[_, _], FieldSelector, Operator)] = node match {
      case contract: ReduceOperator with OneInputKeyedScalaOperator[_, _] => Some((contract.getUDF, contract.key, contract.asInstanceOf[SingleInputOperator[_]].getInput()))
      case contract: ReduceOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, new FieldSelector(contract.getUDF.inputUDT, Nil), contract.asInstanceOf[SingleInputOperator[_]].getInput()))
      case _                                   => None
    }
  }
 object DeltaIterationNode {
    def unapply(node: Operator): Option[(UDF0[_], FieldSelector, Operator, Operator)] = node match {
        case contract: DeltaIteration with DeltaIterationScalaOperator[_] => Some((contract.getUDF, contract.key, contract.asInstanceOf[DualInputOperator[_]].getFirstInput(), contract.asInstanceOf[DualInputOperator[_]].getSecondInput()))
        case _                                  => None
      }
  }
  
  object BulkIterationNode {
    def unapply(node: Operator): Option[(UDF0[_], Operator)] = node match {
      case contract: BulkIteration with BulkIterationScalaOperator[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_]].getInput()))
      case _ => None
    }
  } 
}
