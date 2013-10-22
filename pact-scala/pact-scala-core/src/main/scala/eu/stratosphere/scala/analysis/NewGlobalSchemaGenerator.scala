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

package eu.stratosphere.scala.analysis

import java.util.{List => JList}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.pact.generic.contract.DualInputContract
import eu.stratosphere.pact.generic.contract.SingleInputContract
import eu.stratosphere.scala.analysis.FieldSet.toSeq
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.FileDataSink
import eu.stratosphere.pact.common.contract.GenericDataSource
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.scala.TwoInputScalaContract
import eu.stratosphere.pact.common.contract.MatchContract
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.pact.common.stubs.CoGroupStub
import eu.stratosphere.pact.common.contract.CoGroupContract
import eu.stratosphere.scala.UnionScalaContract
import eu.stratosphere.scala.operators.CopyOperator
import eu.stratosphere.pact.generic.contract.BulkIteration
import eu.stratosphere.scala.BulkIterationScalaContract
import eu.stratosphere.pact.common.contract.GenericDataSink
import eu.stratosphere.scala.WorksetIterationScalaContract
import eu.stratosphere.pact.generic.contract.WorksetIteration

object NewGlobalSchemaGenerator {

  def initGlobalSchema(sinks: Seq[Contract with ScalaContract[_]]): Unit = {

    sinks.foldLeft(0) { (freePos, contract) => globalizeContract(contract, Seq(), Map(), None, freePos) }
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
  private def globalizeContract(contract: Contract, parentInputs: Seq[FieldSet[InputField]], proxies: Map[Contract, Contract with ScalaContract[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    val contract4s = proxies.getOrElse(contract, contract.asInstanceOf[Contract with ScalaContract[_]])

    parentInputs.foreach(contract4s.getUDF.attachOutputsToInputs)

    contract4s.getUDF.outputFields.isGlobalized match {

      case true => freePos

      case false => {

        val freePos1 = globalizeContract(contract4s, proxies, fixedOutputs, freePos)

        eliminateNoOps(contract4s)
        contract4s.persistConfiguration(None)

        freePos1
      }
    }
  }

  private def globalizeContract(contract: Contract with ScalaContract[_], proxies: Map[Contract, Contract with ScalaContract[_]], fixedOutputs: Option[FieldSet[Field]], freePos: Int): Int = {

    contract match {

      case contract : FileDataSink with ScalaContract[_] => {
        contract.getUDF.outputFields.setGlobalized()
        globalizeContract(contract.getInputs().get(0), Seq(contract.getUDF.asInstanceOf[UDF1[_,_]].inputFields), proxies, None, freePos)
      }

      case contract: GenericDataSource[_] with ScalaContract[_] => {
        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : BulkIteration with BulkIterationScalaContract[_] => {

        val s0contract = proxies.getOrElse(contract.getInputs().get(0), contract.getInputs().get(0).asInstanceOf[Contract with ScalaContract[_]])
        val newProxies = proxies + (contract.getPartialSolution() -> s0contract)

        val freePos1 = globalizeContract(contract.getInputs().get(0), Seq(), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(contract.getNextPartialSolution(), Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos1)
        val freePos3 = Option(contract.getTerminationCriterion()) map { globalizeContract(_, Seq(), newProxies, None, freePos2) } getOrElse freePos2

        contract.getUDF.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos3
      }

      case contract : WorksetIteration with WorksetIterationScalaContract[_] => {
        
        val s0 = contract.getInitialSolutionSet().get(0)
        val ws0 = contract.getInitialWorkset().get(0)
        
        val deltaS = contract.getSolutionSetDelta()
        val newWS = contract.getNextWorkset()

        val s0contract = proxies.getOrElse(s0, s0.asInstanceOf[Contract with ScalaContract[_]])
        val ws0contract = proxies.getOrElse(ws0, ws0.asInstanceOf[Contract with ScalaContract[_]])
        val newProxies = proxies + (contract.getSolutionSet() -> s0contract) + (contract.getWorkset() -> ws0contract)

        val freePos1 = globalizeContract(s0, Seq(contract.key.inputFields), proxies, fixedOutputs, freePos)
        val freePos2 = globalizeContract(ws0, Seq(), proxies, None, freePos1)
        val freePos3 = globalizeContract(deltaS, Seq(), newProxies, Some(s0contract.getUDF.outputFields), freePos2)
        val freePos4 = globalizeContract(newWS, Seq(), newProxies, Some(ws0contract.getUDF.outputFields), freePos3)

        contract.getUDF.assignOutputGlobalIndexes(s0contract.getUDF.outputFields)

        freePos4
      }

      case contract : CoGroupContract with TwoInputKeyedScalaContract[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInputs().get(0), Seq(contract.getUDF.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInputs().get(0), Seq(contract.getUDF.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract: CrossContract with TwoInputScalaContract[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInputs().get(0), Seq(contract.getUDF.leftInputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInputs().get(0), Seq(contract.getUDF.rightInputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : MatchContract with TwoInputKeyedScalaContract[_, _, _] => {

        val freePos1 = globalizeContract(contract.getFirstInputs().get(0), Seq(contract.getUDF.leftInputFields, contract.leftKey.inputFields), proxies, None, freePos)
        val freePos2 = globalizeContract(contract.getSecondInputs().get(0), Seq(contract.getUDF.rightInputFields, contract.rightKey.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : MapContract with OneInputScalaContract[_, _] => {

        val freePos1 = globalizeContract(contract.getInputs().get(0), Seq(contract.getUDF.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : ReduceContract with OneInputKeyedScalaContract[_, _] => {

        val freePos1 = globalizeContract(contract.getInputs().get(0), Seq(contract.getUDF.inputFields, contract.key.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }
      
      case contract : ReduceContract with OneInputScalaContract[_, _] => {

        val freePos1 = globalizeContract(contract.getInputs().get(0), Seq(contract.getUDF.inputFields), proxies, None, freePos)

        contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
      }

      case contract : MapContract with UnionScalaContract[_] => {

        // Determine where this contract's children should write their output 
        val freePos1 = contract.getUDF.setOutputGlobalIndexes(freePos, fixedOutputs)
        
        val inputs = contract.getInputs()

        // If an input hasn't yet allocated its output fields, then we can force them into 
        // the expected position. Otherwise, the output fields must be physically copied.
        for (idx <- 0 until inputs.size()) {
          val input = inputs.get(idx)
          val input4s = proxies.getOrElse(input, input.asInstanceOf[Contract with ScalaContract[_]])

//          if (input4s.getUDF.outputFields.isGlobalized || input4s.getUDF.outputFields.exists(_.globalPos.isReference)) {
////            inputs.set(idx, CopyOperator(input4s))
////            throw new RuntimeException("Copy operator needed, not yet implemented properly.")
//            println("Copy operator needed, not yet implemented properly.")
//            println(input4s + " is globalized: " + input4s.getUDF.outputFields.isGlobalized)
//            println(input4s + " has global field.isReference: " + input4s.getUDF.outputFields.exists(_.globalPos.isReference))
//          }
        }

        inputs.foldLeft(freePos) { (freePos2, input) =>
          globalizeContract(input, Seq(), proxies, Some(contract.getUDF.outputFields), freePos)
        }
      }
    }
  }

  private def eliminateNoOps(contract: Contract): Unit = {

    def elim(children: JList[Contract]): Unit = {

      val newChildren = children flatMap {
        case c: MapContract with UnionScalaContract[_] => c.getInputs()
        case child                         => List(child)
      }

      children.clear()
      children.addAll(newChildren)
    }

    contract match {
      case c: SingleInputContract[_] => elim(c.getInputs())
      case c: DualInputContract[_]   => elim(c.getFirstInputs()); elim(c.getSecondInputs())
      case c: GenericDataSink => elim(c.getInputs())
      case _                         =>
    }
  }
}