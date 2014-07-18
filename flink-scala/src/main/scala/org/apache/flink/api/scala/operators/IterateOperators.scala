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


package org.apache.flink.api.scala.operators

import language.experimental.macros
import scala.reflect.macros.Context

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.codegen.MacroContextHolder
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.analysis.UDT
import org.apache.flink.api.scala.analysis.UDT.NothingUDT
import org.apache.flink.api.scala.analysis.UDF1
import org.apache.flink.api.scala.analysis.UDTSerializer
import org.apache.flink.api.scala.analysis.UDF0
import org.apache.flink.api.scala.analysis.FieldSelector

import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.java.record.operators.BulkIteration
import org.apache.flink.api.common.operators.base.BulkIterationBase
import org.apache.flink.api.java.record.operators.DeltaIteration
import org.apache.flink.api.common.operators.base.BulkIterationBase.{TerminationCriterionAggregationConvergence, TerminationCriterionAggregator, TerminationCriterionMapper}
import org.apache.flink.api.common.operators.base.MapOperatorBase
import org.apache.flink.types.NothingTypeInfo
import org.apache.flink.types.{Nothing => JavaNothing}
import org.apache.flink.api.java.typeutils.RecordTypeInfo
import org.apache.flink.api.common.operators.{UnaryOperatorInformation, Operator}
import org.apache.flink.api.java.record.operators.MapOperator
import org.apache.flink.types.Record
import org.apache.flink.api.java.record.functions.MapFunction
import org.apache.flink.util.Collector

object IterateMacros {

  def iterateWithDelta[SolutionItem: c.WeakTypeTag, DeltaItem: c.WeakTypeTag](c: Context { type PrefixType = DataSet[SolutionItem] })(stepFunction: c.Expr[DataSet[SolutionItem] => (DataSet[SolutionItem], DataSet[DeltaItem])]): c.Expr[DataSet[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]
    val (udtDelta, createUdtDelta) = slave.mkUdtClass[DeltaItem]

    val contract = reify {
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val contract = new BulkIteration with BulkIterationScalaOperator[SolutionItem] {
        val udf = new UDF0[SolutionItem](solutionUDT)
        override def getUDF = udf
        private val inputPlaceHolder2 = new BulkIteration.PartialSolutionPlaceHolder(this) with ScalaOperator[SolutionItem, Record] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf
          
        }
        override def getPartialSolution: Operator[Record] = inputPlaceHolder2.asInstanceOf[Operator[Record]]
      }
      
      val partialSolution = new DataSet(contract.getPartialSolution().asInstanceOf[Operator[Record] with ScalaOperator[SolutionItem, Record]])

      val (output, term) = stepFunction.splice.apply(partialSolution)

      contract.setInput(c.prefix.splice.contract)
      contract.setNextPartialSolution(output.contract)

      // is currently not implemented in flink
//      if (term != null) contract.setTerminationCriterion(term)

      new DataSet(contract)
    }

    val result = c.Expr[DataSet[SolutionItem]](Block(List(udtSolution, udtDelta), contract.tree))

    return result
  }
  
  def iterate[SolutionItem: c.WeakTypeTag](c: Context { type PrefixType = DataSet[SolutionItem] })(n: c.Expr[Int], stepFunction: c.Expr[DataSet[SolutionItem] => DataSet[SolutionItem]]): c.Expr[DataSet[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]

    val contract = reify {
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val contract = new BulkIteration with BulkIterationScalaOperator[SolutionItem] {
        val udf = new UDF0[SolutionItem](solutionUDT)
        override def getUDF = udf
        private val inputPlaceHolder2 = new BulkIteration.PartialSolutionPlaceHolder(this) with ScalaOperator[SolutionItem, Record] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf
          
        }
        override def getPartialSolution: Operator[Record] = inputPlaceHolder2.asInstanceOf[Operator[Record]]
      }
      
      val partialSolution = new DataSet(contract.getPartialSolution().asInstanceOf[Operator[Record] with ScalaOperator[SolutionItem, Record]])

      val output = stepFunction.splice.apply(partialSolution)

      contract.setInput(c.prefix.splice.contract)
      contract.setNextPartialSolution(output.contract)
      contract.setMaximumNumberOfIterations(n.splice)

      new DataSet(contract)
    }

    val result = c.Expr[DataSet[SolutionItem]](Block(List(udtSolution), contract.tree))

    return result
  }

  def iterateWithTermination[SolutionItem: c.WeakTypeTag, TerminationItem: c.WeakTypeTag](c: Context { type
  PrefixType = DataSet[SolutionItem] })(n: c.Expr[Int], stepFunction: c.Expr[DataSet[SolutionItem] => 
    DataSet[SolutionItem]], terminationFunction: c.Expr[(DataSet[SolutionItem], 
    DataSet[SolutionItem]) => DataSet[TerminationItem]]): c.Expr[DataSet[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]
    val (udtTermination, createUdtTermination) = slave.mkUdtClass[TerminationItem]

    val contract = reify {
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val terminationUDT = c.Expr[UDT[TerminationItem]](createUdtTermination).splice
      val contract = new BulkIteration with BulkIterationScalaOperator[SolutionItem] {
        val udf = new UDF0[SolutionItem](solutionUDT)
        override def getUDF = udf
        private val inputPlaceHolder2 = new BulkIteration.PartialSolutionPlaceHolder(this) with ScalaOperator[SolutionItem, Record] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf

        }
        override def getPartialSolution: Operator[Record] = inputPlaceHolder2.asInstanceOf[Operator[Record]]
      }

      val partialSolution = new DataSet(contract.getPartialSolution().asInstanceOf[Operator[Record] with ScalaOperator[SolutionItem, Record]])

      val output = stepFunction.splice.apply(partialSolution)
      val terminationCriterion = terminationFunction.splice.apply(partialSolution, output)


      contract.setInput(c.prefix.splice.contract)
      contract.setNextPartialSolution(output.contract)
      contract.setMaximumNumberOfIterations(n.splice)
      contract.setTerminationCriterion(terminationCriterion.contract)

      new DataSet(contract)
    }

    val result = c.Expr[DataSet[SolutionItem]](Block(List(udtSolution, udtTermination), contract.tree))

    return result
  }
}


object WorksetIterateMacros {

   
  def iterateWithDelta[SolutionItem: c.WeakTypeTag, SolutionKey: c.WeakTypeTag, WorksetItem: c.WeakTypeTag](c: Context { type PrefixType = DataSet[SolutionItem] })(workset: c.Expr[DataSet[WorksetItem]], solutionSetKey: c.Expr[SolutionItem => SolutionKey], stepFunction: c.Expr[(DataSet[SolutionItem], DataSet[WorksetItem]) => (DataSet[SolutionItem], DataSet[WorksetItem])], maxIterations: c.Expr[Int]): c.Expr[DataSet[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]
    val (udtWorkset, createUdtWorkset) = slave.mkUdtClass[WorksetItem]

    val keySelection = slave.getSelector(solutionSetKey)

    val contract = reify {
      
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val worksetUDT = c.Expr[UDT[WorksetItem]](createUdtWorkset).splice
      
      val keySelector = new FieldSelector(solutionUDT, keySelection.splice)
      val keyFields = keySelector.selectedFields
      val keyPositions = keyFields.toIndexArray

      val contract = new DeltaIteration(keyPositions) with DeltaIterationScalaOperator[SolutionItem] {
        override val key = keySelector
        val udf = new UDF0[SolutionItem](solutionUDT)     
        override def getUDF = udf

        private val solutionSetPlaceHolder2 = new DeltaIteration.SolutionSetPlaceHolder(this) with ScalaOperator[SolutionItem, Record] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf

        }
        override def getSolutionSet: Operator[Record] = solutionSetPlaceHolder2.asInstanceOf[Operator[Record]]
        
        private val worksetPlaceHolder2 = new DeltaIteration.WorksetPlaceHolder(this) with ScalaOperator[WorksetItem, Record] with Serializable {
          val udf = new UDF0[WorksetItem](worksetUDT)
          override def getUDF = udf

        }
        override def getWorkset: Operator[Record] = worksetPlaceHolder2.asInstanceOf[Operator[Record]]
      }

      val solutionInput = new DataSet(contract.getSolutionSet().asInstanceOf[Operator[Record] with ScalaOperator[SolutionItem, Record]])
      val worksetInput = new DataSet(contract.getWorkset().asInstanceOf[Operator[Record] with ScalaOperator[WorksetItem, Record]])


      contract.setInitialSolutionSet(c.prefix.splice.contract)
      contract.setInitialWorkset(workset.splice.contract)

      val (delta, nextWorkset) = stepFunction.splice.apply(solutionInput, worksetInput)
      contract.setSolutionSetDelta(delta.contract)
      contract.setNextWorkset(nextWorkset.contract)
      contract.setMaximumNumberOfIterations(maxIterations.splice)

      new DataSet(contract)
    }
    
    val result = c.Expr[DataSet[SolutionItem]](Block(List(udtSolution, udtWorkset), contract.tree))

    return result
  }
}
