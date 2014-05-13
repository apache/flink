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
import eu.stratosphere.api.common.operators.DualInputOperator
import eu.stratosphere.api.common.operators.SingleInputOperator
import eu.stratosphere.api.java.record.operators._
import eu.stratosphere.api.scala._
import eu.stratosphere.api.common.operators.base.{BulkIterationBase => BulkIteration, DeltaIterationBase => DeltaIteration, GenericDataSinkBase, GenericDataSourceBase}
import eu.stratosphere.api.common.operators.Union
import eu.stratosphere.types.Record
import eu.stratosphere.types.{Nothing => JavaNothing}
import scala.Some
import scala.Some

object Extractors {

  object DataSinkNode {
    def unapply(node: Operator[JavaNothing]): Option[(UDF1[_, _], Operator[Record])] = node match {
      case contract: GenericDataSinkBase[_] with ScalaOutputOperator[_] => {
        Some((contract.getUDF.asInstanceOf[UDF1[_, _]], node.asInstanceOf[GenericDataSinkBase[_]].getInput().asInstanceOf[Operator[Record]]))
      }
      case _                               => None
    }
  }

  object DataSourceNode {
    def unapply(node: Operator[Record]): Option[(UDF0[_])] = node match {
      case contract: GenericDataSourceBase[_, _] with ScalaOperator[_, _] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
      case _                                 => None
    }
  }

  object CoGroupNode {
    def unapply(node: Operator[Record]): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, Operator[Record], Operator[Record])] = node match {
      case contract: CoGroupOperator with TwoInputKeyedScalaOperator[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_, _, _, _]].getFirstInput().asInstanceOf[Operator[Record]], contract.asInstanceOf[DualInputOperator[_, _, _, _]].getSecondInput().asInstanceOf[Operator[Record]]))
      case _                                       => None
    }
  }

  object CrossNode {
    def unapply(node: Operator[Record]): Option[(UDF2[_, _, _], Operator[Record], Operator[Record])] = node match {
      case contract: CrossOperator with TwoInputScalaOperator[_, _, _] => Some((contract.getUDF, contract.asInstanceOf[DualInputOperator[_, _, _, _]].getFirstInput().asInstanceOf[Operator[Record]], contract.asInstanceOf[DualInputOperator[_, _, _, _]].getSecondInput().asInstanceOf[Operator[Record]]))
      case _                                  => None
    }
  }

  object JoinNode {
    def unapply(node: Operator[Record]): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, Operator[Record], Operator[Record])] = node match {
      case contract: JoinOperator with TwoInputKeyedScalaOperator[ _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputOperator[_, _, _, _]].getFirstInput().asInstanceOf[Operator[Record]], contract.asInstanceOf[DualInputOperator[_, _, _, _]].getSecondInput().asInstanceOf[Operator[Record]]))
      case _                                    => None
    }
  }

  object MapNode {
    def unapply(node: Operator[Record]): Option[(UDF1[_, _], Operator[Record])] = node match {
      case contract: MapOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_, _, _]].getInput().asInstanceOf[Operator[Record]]))
      case _                             => None
    }
  }
  
  object UnionNode {
    def unapply(node: Operator[Record]): Option[(UDF2[_, _, _], Operator[Record], Operator[Record])] = node match {
      case contract: Union[_] with UnionScalaOperator[_] => Some((contract.getUDF, contract.asInstanceOf[DualInputOperator[_, _, _, _]].getFirstInput().asInstanceOf[Operator[Record]], contract.asInstanceOf[DualInputOperator[_, _, _, _]].getSecondInput().asInstanceOf[Operator[Record]]))
      case _                             => None
    }
  }

  object ReduceNode {
    def unapply(node: Operator[Record]): Option[(UDF1[_, _], FieldSelector, Operator[Record])] = node match {
      case contract: ReduceOperator with OneInputKeyedScalaOperator[_, _] => Some((contract.getUDF, contract.key, contract.asInstanceOf[SingleInputOperator[_, _, _]].getInput().asInstanceOf[Operator[Record]]))
      case contract: ReduceOperator with OneInputScalaOperator[_, _] => Some((contract.getUDF, new FieldSelector(contract.getUDF.inputUDT, Nil), contract.asInstanceOf[SingleInputOperator[_, _, _]].getInput().asInstanceOf[Operator[Record]]))
      case _                                   => None
    }
  }
 object DeltaIterationNode {
    def unapply(node: Operator[Record]): Option[(UDF0[_], FieldSelector, Operator[Record], Operator[Record])] = node match {
        case contract: DeltaIteration[_, _] with DeltaIterationScalaOperator[_] => Some((contract.getUDF, contract.key, contract.asInstanceOf[DualInputOperator[_, _, _, _]].getFirstInput().asInstanceOf[Operator[Record]], contract.asInstanceOf[DualInputOperator[_, _, _, _]].getSecondInput().asInstanceOf[Operator[Record]]))
        case _                                  => None
      }
  }
  
  object BulkIterationNode {
    def unapply(node: Operator[Record]): Option[(UDF0[_], Operator[Record])] = node match {
      case contract: BulkIteration[_] with BulkIterationScalaOperator[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputOperator[_, _, _]].getInput().asInstanceOf[Operator[Record]]))
      case _ => None
    }
  } 
}
