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

import java.util.{List => JList}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.common.operators.DualInputOperator
import eu.stratosphere.api.common.operators.SingleInputOperator
import eu.stratosphere.api.scala.analysis.FieldSet.toSeq
import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.common.operators.FileDataSink
import eu.stratosphere.api.common.operators.GenericDataSource
import eu.stratosphere.api.java.record.operators.MapOperator
import eu.stratosphere.api.scala.OneInputScalaOperator
import eu.stratosphere.api.java.record.operators.ReduceOperator
import eu.stratosphere.api.scala.OneInputKeyedScalaOperator
import eu.stratosphere.api.java.record.operators.CrossOperator
import eu.stratosphere.api.scala.TwoInputScalaOperator
import eu.stratosphere.api.java.record.operators.JoinOperator
import eu.stratosphere.api.scala.TwoInputKeyedScalaOperator
import eu.stratosphere.api.java.record.operators.CoGroupOperator
import eu.stratosphere.api.scala.UnionScalaOperator
import eu.stratosphere.api.common.operators.BulkIteration
import eu.stratosphere.api.scala.BulkIterationScalaOperator
import eu.stratosphere.api.common.operators.DeltaIteration
import eu.stratosphere.api.scala.DeltaIterationScalaOperator
import eu.stratosphere.api.common.operators.GenericDataSink
import eu.stratosphere.api.common.operators.base.MapOperatorBase
import eu.stratosphere.api.common.operators.Union

class GlobalSchemaGenerator {

  def initGlobalSchema(sinks: Seq[Operator with ScalaOperator[_]]): Unit = {
    // don't do anything, we don't need global positions if we don't do reordering of operators
    // FieldSet.toSerializerIndexArray returns local positions and ignores global positions
    // sinks.foldLeft(0) { (freePos, contract) => globalizeContract(contract, Seq(), Map(), None, freePos) }
  }

  /**
   * Computes disjoint write sets for a contract and its inputs.
   *
   * @param contract The contract to globalize
   * @param parentInputs Input fields which should be bound to the contract's outputs
   * @param proxies Provides contracts for iteration placeholders
   * @param fixedOutputs Specifies required positions for the contract's output fields, or None to allocate new positions
   * @param freePos The current first available position in the global schema
   * @return The new first available position in the global schema
   */
  private def globalizeContract(contract: Operator, parentInputs: Seq[FieldSet[InputField]], proxies: Map[Operator, Operator with ScalaOperator[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    val contract4s = proxies.getOrElse(contract, contract.asInstanceOf[Operator with ScalaOperator[_]])

    parentInputs.foreach(contract4s.getUDF.attachOutputsToInputs)

    contract4s.getUDF.outputFields.isGlobalized match {

      case true => freePos

      case false => {

        val freePos1 = globalizeContract(contract4s, proxies, fixedOutputs, freePos)

        contract4s.persistConfiguration(None)

        freePos1
      }
    }
  }

  private def globalizeContract(contract: Operator with ScalaOperator[_], proxies: Map[Operator, Operator with ScalaOperator[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    contract match {

      case contract : FileDataSink with ScalaOperator[_] => {
        contract.getUDF.outputFields.setGlobalized()
        globalizeContract(contract.getInput(), Seq(contract.getUDF.asInstanceOf[UDF1[_,_]].inputFields), proxies, None, freePos)
      }

      case contract: GenericDataSource[_] with ScalaOperator[_] => {
        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : BulkIteration with BulkIterationScalaOperator[_] => {
        val s0 = contract.getInput()

        val s0contract = proxies.getOrElse(s0, s0.asInstanceOf[Operator with ScalaOperator[_]])
        val newProxies = proxies + (contract.getPartialSolution() -> s0contract)

        val freePos1 = globalizeContract(s0, Seq(), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(contract.getNextPartialSolution(), Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos1)
        val freePos3 = Option(contract.getTerminationCriterion()) map { globalizeContract(_, Seq(), newProxies, None, freePos2) } getOrElse freePos2

        contract.getUDF.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos3
      }

      case contract : DeltaIteration with DeltaIterationScalaOperator[_] => {
//      case contract @ WorksetIterate4sContract(s0, ws0, deltaS, newWS, placeholderS, placeholderWS) => {
        val s0 = contract.getInitialSolutionSet()
        val ws0 = contract.getInitialWorkset()
        val deltaS = contract.getSolutionSetDelta
        val newWS = contract.getNextWorkset

        val s0contract = proxies.getOrElse(s0, s0.asInstanceOf[Operator with ScalaOperator[_]])
        val ws0contract = proxies.getOrElse(ws0, ws0.asInstanceOf[Operator with ScalaOperator[_]])
        val newProxies = proxies + (contract.getSolutionSetDelta -> s0contract) + (contract.getNextWorkset -> ws0contract)

        val freePos1 = globalizeContract(s0, Seq(contract.key.inputFields), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(ws0, Seq(), proxies, None, freePos1)
        val freePos3 = globalizeContract(deltaS, Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos2)
        val freePos4 = globalizeContract(newWS, Seq(), newProxies, Some(ws0contract.getUDF.outputFields), freePos3)

        contract.getUDF.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos4
      }

      case contract : CoGroupOperator with TwoInputKeyedScalaOperator[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInput(), Seq(contract.getUDF.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInput(), Seq(contract.getUDF.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos1)

        contract.getUDF.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract: CrossOperator with TwoInputScalaOperator[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInput(), Seq(contract.getUDF.leftInputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInput(), Seq(contract.getUDF.rightInputFields), proxies, None, freePos1)

        contract.getUDF.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract : JoinOperator with TwoInputKeyedScalaOperator[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInput(), Seq(contract.getUDF.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInput(), Seq(contract.getUDF.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos1)

        contract.getUDF.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }

      case contract : MapOperatorBase[_] with OneInputScalaOperator[_, _] => {

        val freePos1 = globalizeContract(contract.getInput(), Seq(contract.getUDF.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }

      case contract : ReduceOperator with OneInputKeyedScalaOperator[_, _] => {

        val freePos1 = globalizeContract(contract.getInput(), Seq(contract.getUDF.inputFields, contract.key.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }

      // for key-less (global) reducers
      case contract : ReduceOperator with OneInputScalaOperator[_, _] => {

        val freePos1 = globalizeContract(contract.getInput(), Seq(contract.getUDF.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos1, fixedOutputs)
      }

      case contract : Union with UnionScalaOperator[_] => {
        
        val freePos1 = globalizeContract(contract.getFirstInput(), Seq(contract.getUDF.leftInputFields), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(contract.getSecondInput(), Seq(contract.getUDF.rightInputFields), proxies, fixedOutputs, freePos1)

        contract.getUDF.setOutputGlobalIndexes(freePos2, fixedOutputs)
      }
    }
  }
}