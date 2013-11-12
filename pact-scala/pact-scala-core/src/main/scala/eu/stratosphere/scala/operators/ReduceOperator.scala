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

import java.util.{ Iterator => JIterator }

import language.experimental.macros
import scala.reflect.macros.Context

import eu.stratosphere.nephele.configuration.Configuration

import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.stubs.ReduceStub

import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.operators.stubs.DeserializingIterator
import eu.stratosphere.scala.codegen.MacroContextHolder
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.scala.DataSet
import eu.stratosphere.scala.OneInputHintable
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.codegen.Util

class GroupByDataStream[In](val keySelection: List[Int], val input: DataSet[In]) {
  def reduceGroup[Out](fun: Iterator[In] => Out): DataSet[Out] with OneInputHintable[In, Out] = macro ReduceMacros.reduceGroup[In, Out]
  // def combinableReduceGroup(fun: Iterator[In] => In): DataStream[In] with OneInputHintable[In, In] = macro ReduceMacros.combinableReduce[In]
  
  def reduce(fun: (In, In) => In): DataSet[In] with OneInputHintable[In, In] = macro ReduceMacros.reduce[In]
  
  def count() : DataSet[(In, Int)] with OneInputHintable[In, (In, Int)] = macro ReduceMacros.count[In]
}

object ReduceMacros {
  
  def groupBy[In: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(keyFun: c.Expr[In => Key]): c.Expr[GroupByDataStream[In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
    	new GroupByDataStream[In](keySelection.splice, c.prefix.splice)
    }

    return helper
  }
  
  def reduce[In: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[(In, In) => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
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
        private var combineForwardFrom: Array[Int] = _
        private var combineForwardTo: Array[Int] = _

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[In] = _
        private var reduceForwardFrom: Array[Int] = _
        private var reduceForwardTo: Array[Int] = _

        private def combinerOutputs: Set[Int] = udf.inputFields.filter(_.isUsed).map(_.globalPos.getValue).toSet
        private def forwardedKeys: Set[Int] = keySelector.selectedFields.toIndexSet.diff(combinerOutputs)

        def combineForwardSetFrom: Set[Int] = udf.getForwardIndexSetFrom.diff(combinerOutputs).union(forwardedKeys).toSet
        def combineForwardSetTo: Set[Int] = udf.getForwardIndexSetTo.diff(combinerOutputs).union(forwardedKeys).toSet
        def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(combinerOutputs).diff(forwardedKeys).toSet

        private def combineOutputLength = {
          val outMax = if (combinerOutputs.isEmpty) -1 else combinerOutputs.max
          val forwardMax = if (combineForwardSetTo.isEmpty) -1 else combineForwardSetTo.max
          math.max(outMax, forwardMax) + 1
        }

        override def open(config: Configuration) = {
          super.open(config)
          this.combineRecord.setNumFields(combineOutputLength)
          this.reduceRecord.setNumFields(udf.getOutputLength)

          this.combineIterator = new DeserializingIterator(udf.getInputDeserializer)
          // we are serializing for the input of the reduce...
          this.combineSerializer = udf.getInputDeserializer
          this.combineForwardFrom = combineForwardSetFrom.toArray
          this.combineForwardTo = combineForwardSetTo.toArray

          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForwardFrom = udf.getForwardIndexArrayFrom.toArray
          this.reduceForwardTo = udf.getForwardIndexArrayTo.toArray
        }
        
        val userCode = fun.splice

        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = combineIterator.initialize(records)
          combineRecord.copyFrom(firstRecord, combineForwardFrom, combineForwardTo)

          val output = combineIterator.reduce(userCode)

          combineSerializer.serialize(output, combineRecord)
          out.collect(combineRecord)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = reduceIterator.reduce(userCode)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)

      val keyPositions = generatedStub.keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def reduceGroup[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[Iterator[In] => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
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
        private var reduceForwardFrom: Array[Int] = _
        private var reduceForwardTo: Array[Int] = _

        override def open(config: Configuration) = {
          super.open(config)
          this.reduceRecord.setNumFields(udf.getOutputLength)
          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForwardFrom = udf.getForwardIndexArrayFrom
          this.reduceForwardTo = udf.getForwardIndexArrayTo
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = fun.splice.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)

      val keyPositions = generatedStub.keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, Out] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with OneInputHintable[In, Out] {}
    }

    val result = c.Expr[DataSet[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
  
  def combinableReduce[In: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })(fun: c.Expr[Iterator[In] => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
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
        private var combineForwardFrom: Array[Int] = _
        private var combineForwardTo: Array[Int] = _

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[In] = _
        private var reduceForwardFrom: Array[Int] = _
        private var reduceForwardTo: Array[Int] = _

        private def combinerOutputs: Set[Int] = udf.inputFields.filter(_.isUsed).map(_.globalPos.getValue).toSet
        private def forwardedKeys: Set[Int] = keySelector.selectedFields.toIndexSet.diff(combinerOutputs)

        def combineForwardSetFrom: Set[Int] = udf.getForwardIndexSetFrom.diff(combinerOutputs).union(forwardedKeys).toSet
        def combineForwardSetTo: Set[Int] = udf.getForwardIndexSetTo.diff(combinerOutputs).union(forwardedKeys).toSet
        def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(combinerOutputs).diff(forwardedKeys).toSet

        private def combineOutputLength = {
          val outMax = if (combinerOutputs.isEmpty) -1 else combinerOutputs.max
          val forwardMax = if (combineForwardSetTo.isEmpty) -1 else combineForwardSetTo.max
          math.max(outMax, forwardMax) + 1
        }

        override def open(config: Configuration) = {
          super.open(config)
          this.combineRecord.setNumFields(combineOutputLength)
          this.reduceRecord.setNumFields(udf.getOutputLength)

          this.combineIterator = new DeserializingIterator(udf.getInputDeserializer)
          // we are serializing for the input of the reduce...
          this.combineSerializer = udf.getInputDeserializer
          this.combineForwardFrom = combineForwardSetFrom.toArray
          this.combineForwardTo = combineForwardSetTo.toArray

          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForwardFrom = udf.getForwardIndexArrayFrom
          this.reduceForwardTo = udf.getForwardIndexArrayTo
        }
        
        val userCode = fun.splice

        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = combineIterator.initialize(records)
          combineRecord.copyFrom(firstRecord, combineForwardFrom, combineForwardTo)

          val output = userCode.apply(combineIterator)

          combineSerializer.serialize(output, combineRecord)
          out.collect(combineRecord)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = userCode.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)

      val keyPositions = generatedStub.keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, In] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def globalReduce[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[(In, In) => In]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    
    val contract = reify {

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = inputUDT
        val udf: UDF1[In, In] = new UDF1(inputUDT, outputUDT)
        
        private val combineRecord = new PactRecord()
        private val reduceRecord = new PactRecord()

        private var combineIterator: DeserializingIterator[In] = null
        private var combineSerializer: UDTSerializer[In] = _
        private var combineForwardFrom: Array[Int] = _
        private var combineForwardTo: Array[Int] = _

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[In] = _
        private var reduceForwardFrom: Array[Int] = _
        private var reduceForwardTo: Array[Int] = _

        private def combinerOutputs: Set[Int] = udf.inputFields.filter(_.isUsed).map(_.globalPos.getValue).toSet

        def combineForwardSetFrom: Set[Int] = udf.getForwardIndexSetFrom.diff(combinerOutputs).toSet
        def combineForwardSetTo: Set[Int] = udf.getForwardIndexSetTo.diff(combinerOutputs).toSet
        def combineDiscardSet: Set[Int] = udf.discardSet.map(_.getValue).diff(combinerOutputs).toSet

        private def combineOutputLength = {
          val outMax = if (combinerOutputs.isEmpty) -1 else combinerOutputs.max
          val forwardMax = if (combineForwardSetTo.isEmpty) -1 else combineForwardSetTo.max
          math.max(outMax, forwardMax) + 1
        }

        override def open(config: Configuration) = {
          super.open(config)
          this.combineRecord.setNumFields(combineOutputLength)
          this.reduceRecord.setNumFields(udf.getOutputLength)

          this.combineIterator = new DeserializingIterator(udf.getInputDeserializer)
          // we are serializing for the input of the reduce...
          this.combineSerializer = udf.getInputDeserializer
          this.combineForwardFrom = combineForwardSetFrom.toArray
          this.combineForwardTo = combineForwardSetTo.toArray

          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForwardFrom = udf.getForwardIndexArrayFrom
          this.reduceForwardTo = udf.getForwardIndexArrayTo
        }
        
        val userCode = fun.splice

        override def combine(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = combineIterator.initialize(records)
          combineRecord.copyFrom(firstRecord, combineForwardFrom, combineForwardTo)

          val output = combineIterator.reduce(userCode)

          combineSerializer.serialize(output, combineRecord)
          out.collect(combineRecord)
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = reduceIterator.reduce(userCode)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(c.prefix.splice.contract)
      
      val ret = new ReduceContract(builder) with OneInputScalaContract[In, In] {
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[In](ret) with OneInputHintable[In, In] {}
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))
    
    return result
  }

  def globalReduceAll[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[Iterator[In] => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)
        
        private val reduceRecord = new PactRecord()

        private var reduceIterator: DeserializingIterator[In] = null
        private var reduceSerializer: UDTSerializer[Out] = _
        private var reduceForwardFrom: Array[Int] = _
        private var reduceForwardTo: Array[Int] = _

        override def open(config: Configuration) = {
          super.open(config)
          this.reduceRecord.setNumFields(udf.getOutputLength)
          this.reduceIterator = new DeserializingIterator(udf.getInputDeserializer)
          this.reduceSerializer = udf.getOutputSerializer
          this.reduceForwardFrom = udf.getForwardIndexArrayFrom
          this.reduceForwardTo = udf.getForwardIndexArrayTo
        }

        override def reduce(records: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstRecord = reduceIterator.initialize(records)
          reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

          val output = fun.splice.apply(reduceIterator)

          reduceSerializer.serialize(output, reduceRecord)
          out.collect(reduceRecord)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(c.prefix.splice.contract)
      
      val ret = new ReduceContract(builder) with OneInputScalaContract[In, Out] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with OneInputHintable[In, Out] {}
    }

    val result = c.Expr[DataSet[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
  
  
  def count[In: c.WeakTypeTag](c: Context { type PrefixType = GroupByDataStream[In] })() : c.Expr[DataSet[(In, Int)] with OneInputHintable[In, (In, Int)]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[(In, Int)]
    
    val contract = reify {
      val helper: GroupByDataStream[In] = c.prefix.splice
      val keySelection = helper.keySelection

      val generatedStub = new ReduceStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[(In, Int)]](createUdtOut).splice
        val keySelector = new FieldSelector(inputUDT, keySelection)
        val udf: UDF1[In, (In, Int)] = new UDF1(inputUDT, outputUDT)
        
        private val reduceRecord = new PactRecord()
        private val pactInt = new PactInteger()

        private var countPosition: Int = 0;

        override def open(config: Configuration) = {
          super.open(config)
          this.countPosition = udf.getOutputLength - 1;
        }
        
        override def reduce(records: JIterator[PactRecord], result: Collector[PactRecord]) : Unit = {
          
          var record : PactRecord = null
          var counter: Int = 0
          while (records.hasNext()) {
            record = records.next()
            val count = if (record.getNumFields() <= countPosition || record.isNull(countPosition)) 1 else record.getField(countPosition, pactInt).getValue()
            counter = counter + count
          }
          
          pactInt.setValue(counter)
          record.setField(countPosition, pactInt)
          result.collect(record)
        }
        
        override def combine(records: JIterator[PactRecord], result: Collector[PactRecord]) : Unit = {
          reduce(records, result)
        }

      }
      
      val builder = ReduceContract.builder(generatedStub).input(helper.input.contract)

      val keyPositions = generatedStub.keySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.inputUDT.getKeySet(keyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), keyPositions(i)) }
      
      val ret = new ReduceContract(builder) with OneInputKeyedScalaContract[In, (In, Int)] {
        override val key: FieldSelector = generatedStub.keySelector
        override def getUDF = generatedStub.udf
        override def annotations = Annotations.getCombinable() +: Seq(Annotations.getConstantFieldsExcept(Array[Int]()))
      }
      new DataSet[(In, Int)](ret) with OneInputHintable[In, (In, Int)] {}
    }

    val result = c.Expr[DataSet[(In, Int)] with OneInputHintable[In, (In, Int)]](Block(List(udtIn, udtOut), contract.tree))
    
    return result
  }
}
