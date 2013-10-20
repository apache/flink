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

package eu.stratosphere.scala.operators

import language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.scala.codegen.MacroContextHolder
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.operators.stubs.DeserializingIterator
import eu.stratosphere.nephele.configuration.Configuration
import java.util.{ Iterator => JIterator }
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.scala.DataStream
import eu.stratosphere.scala.OneInputHintable

class GroupByDataStream[In](val keySelection: List[Int], val input: DataStream[In]) {
  def groupReduce[Out](fun: Iterator[In] => Out): DataStream[Out] with OneInputHintable[In, Out] = macro ReduceMacros.clunkyReduce[In, Out]
  def combinableGroupReduce(fun: Iterator[In] => In): DataStream[In] with OneInputHintable[In, In] = macro ReduceMacros.combinableReduce[In]
  
  def reduce(fun: (In, In) => In): DataStream[In] with OneInputHintable[In, In] = macro ReduceMacros.properReduce[In]
}

object ReduceMacros {
  
  def groupByImpl[In: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = DataStream[In] })(keyFun: c.Expr[In => Key]): c.Expr[GroupByDataStream[In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
    	new GroupByDataStream[In](keySelection.splice, c.prefix.splice)
    }

    return helper
  }
  
  def properReduce[In: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[(In, In) => In]): c.Expr[DataStream[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val contract = reify {
      val helper: GroupByDataStream[In] = c.prefix.splice
      val keySelection = helper.keySelection

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = inputUDT
        val keySelector = new FieldSelector(inputUDT, keySelection)
        val udf: UDF1[In, In] = new UDF1(inputUDT, outputUDT)
        
        private val combineRecord = new PactRecord()
        private val reduceRecord = new PactRecord()

        private var combineIterator: DeserializingIterator[In] = null
        private var combineSerializer: UDTSerializer[In] = _
        private var combineForward: Array[Int] = _

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[In] = _
        private var reduceForward: Array[Int] = _

        private def combinerOutputs: Set[Int] = udf.inputFields.filter(_.isUsed).map(_.globalPos.getValue).toSet
        private def forwardedKeys: Set[Int] = keySelector.selectedFields.toIndexSet.diff(combinerOutputs)

        def combineForwardSet: Set[Int] = udf.forwardSet.map(_.getValue).diff(combinerOutputs).union(forwardedKeys).toSet
        def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(combinerOutputs).diff(forwardedKeys).toSet

        private def combineOutputLength = {
          val outMax = if (combinerOutputs.isEmpty) -1 else combinerOutputs.max
          val forwardMax = if (combineForwardSet.isEmpty) -1 else combineForwardSet.max
          math.max(outMax, forwardMax) + 1
        }

        override def open(config: Configuration) = {
          super.open(config)
          this.combineRecord.setNumFields(combineOutputLength)
          this.reduceRecord.setNumFields(udf.getOutputLength)

          this.combineIterator = new DeserializingIterator(udf.getInputDeserializer)
          // we are serializing for the input of the reduce...
          this.combineSerializer = udf.getInputDeserializer
          this.combineForward = combineForwardSet.toArray

          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForward = udf.getForwardIndexArray
        }
        
        val userCode = fun.splice

        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = combineIterator.initialize(records)
          combineRecord.copyFrom(firstRecord, combineForward, combineForward)

          val output = combineIterator.reduce(userCode)

          combineSerializer.serialize(output, combineRecord)
          out.collect(combineRecord)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForward, reduceForward)

          val output = reduceIterator.reduce(userCode)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)
      
      val keyTypes = generatedStub.inputUDT.getKeySet(generatedStub.keySelector.selectedFields map { _.localPos })
      keyTypes.foreach { builder.keyField(_, -1) } // global indexes haven't been computed yet...
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      new DataStream[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataStream[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def clunkyReduce[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[Iterator[In] => Out]): c.Expr[DataStream[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {
      val helper: GroupByDataStream[In] = c.prefix.splice
      val keySelection = helper.keySelection

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val keySelector = new FieldSelector(inputUDT, keySelection)
        val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)
        
        private val reduceRecord = new PactRecord()

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[Out] = _
        private var reduceForward: Array[Int] = _

        override def open(config: Configuration) = {
          super.open(config)
          this.reduceRecord.setNumFields(udf.getOutputLength)
          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForward = udf.getForwardIndexArray
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForward, reduceForward)

          val output = fun.splice.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)
      
      val keyTypes = generatedStub.inputUDT.getKeySet(generatedStub.keySelector.selectedFields map { _.localPos })
      keyTypes.foreach { builder.keyField(_, -1) } // global indexes haven't been computed yet...
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, Out] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      new DataStream[Out](ret) with OneInputHintable[In, Out] {}
    }

    val result = c.Expr[DataStream[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
  
  def combinableReduce[In: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[Iterator[In] => In]): c.Expr[DataStream[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val contract = reify {
      val helper: GroupByDataStream[In] = c.prefix.splice
      val keySelection = helper.keySelection

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = inputUDT
        val keySelector = new FieldSelector(inputUDT, keySelection)
        val udf: UDF1[In, In] = new UDF1(inputUDT, outputUDT)
        
        private val combineRecord = new PactRecord()
        private val reduceRecord = new PactRecord()

        private var combineIterator: DeserializingIterator[In] = null
        private var combineSerializer: UDTSerializer[In] = _
        private var combineForward: Array[Int] = _

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[In] = _
        private var reduceForward: Array[Int] = _

        private def combinerOutputs: Set[Int] = udf.inputFields.filter(_.isUsed).map(_.globalPos.getValue).toSet
        private def forwardedKeys: Set[Int] = keySelector.selectedFields.toIndexSet.diff(combinerOutputs)

        def combineForwardSet: Set[Int] = udf.forwardSet.map(_.getValue).diff(combinerOutputs).union(forwardedKeys).toSet
        def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(combinerOutputs).diff(forwardedKeys).toSet

        private def combineOutputLength = {
          val outMax = if (combinerOutputs.isEmpty) -1 else combinerOutputs.max
          val forwardMax = if (combineForwardSet.isEmpty) -1 else combineForwardSet.max
          math.max(outMax, forwardMax) + 1
        }

        override def open(config: Configuration) = {
          super.open(config)
          this.combineRecord.setNumFields(combineOutputLength)
          this.reduceRecord.setNumFields(udf.getOutputLength)

          this.combineIterator = new DeserializingIterator(udf.getInputDeserializer)
          // we are serializing for the input of the reduce...
          this.combineSerializer = udf.getInputDeserializer
          this.combineForward = combineForwardSet.toArray

          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForward = udf.getForwardIndexArray
        }
        
        val userCode = fun.splice

        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = combineIterator.initialize(records)
          combineRecord.copyFrom(firstRecord, combineForward, combineForward)

          val output = userCode.apply(combineIterator)

          combineSerializer.serialize(output, combineRecord)
          out.collect(combineRecord)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForward, reduceForward)

          val output = userCode.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)
      
      val keyTypes = generatedStub.inputUDT.getKeySet(generatedStub.keySelector.selectedFields map { _.localPos })
      keyTypes.foreach { builder.keyField(_, -1) } // global indexes haven't been computed yet...
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      new DataStream[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataStream[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }
}
