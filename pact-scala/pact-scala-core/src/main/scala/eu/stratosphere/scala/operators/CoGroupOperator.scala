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
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.scala.TwoInputScalaContract
import eu.stratosphere.scala.analysis.UDF2
import eu.stratosphere.pact.common.stubs.CrossStub
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.pact.common.stubs.MatchStub
import eu.stratosphere.pact.common.contract.CoGroupContract
import eu.stratosphere.pact.common.stubs.CoGroupStub
import eu.stratosphere.scala.DataSet
import eu.stratosphere.pact.generic.contract.UserCodeObjectWrapper
import eu.stratosphere.scala.TwoInputHintable
import eu.stratosphere.scala.codegen.Util

class CoGroupDataSet[LeftIn, RightIn](val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def where[Key](keyFun: LeftIn => Key): CoGroupDataSetWithWhere[LeftIn, RightIn, Key] = macro CoGroupMacros.whereImpl[LeftIn, RightIn, Key]
}

class CoGroupDataSetWithWhere[LeftIn, RightIn, Key](val leftKeySelection: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def isEqualTo[Key](keyFun: RightIn => Key): CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn] = macro CoGroupMacros.isEqualToImpl[LeftIn, RightIn, Key]
}

class CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn](val leftKeySelection: List[Int], val rightKeySelection: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def map[Out](fun: (Iterator[LeftIn], Iterator[RightIn]) => Out): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro CoGroupMacros.map[LeftIn, RightIn, Out]
  def flatMap[Out](fun: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro CoGroupMacros.flatMap[LeftIn, RightIn, Out]
}

class NoKeyCoGroupBuilder(s: CoGroupStub) extends CoGroupContract.Builder(new UserCodeObjectWrapper(s))

object CoGroupMacros {
  
  def whereImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = CoGroupDataSet[LeftIn, RightIn] })(keyFun: c.Expr[LeftIn => Key]): c.Expr[CoGroupDataSetWithWhere[LeftIn, RightIn, Key]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelector = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new CoGroupDataSetWithWhere[LeftIn, RightIn, Key](keySelector.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }
  
  def isEqualToImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = CoGroupDataSetWithWhere[LeftIn, RightIn, Key] })(keyFun: c.Expr[RightIn => Key]): c.Expr[CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelector = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn](helper.leftKeySelection, keySelector.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }

  def map[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(Iterator[LeftIn], Iterator[RightIn]) => Out]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {
      val helper: CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftKeySelection = helper.leftKeySelection
      val rightKeySelection = helper.rightKeySelection

      val generatedStub = new CoGroupStub with Serializable {
        val leftInputUDT = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
        val rightInputUDT = c.Expr[UDT[RightIn]](createUdtRightIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val leftKeySelector = new FieldSelector(leftInputUDT, leftKeySelection)
        val rightKeySelector = new FieldSelector(rightInputUDT, rightKeySelection)
        val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)
        
        private val outputRecord = new PactRecord()

        private var leftIterator: DeserializingIterator[LeftIn] = _
        private var leftForwardFrom: Array[Int] = _
        private var leftForwardTo: Array[Int] = _
        private var rightIterator: DeserializingIterator[RightIn] = _
        private var rightForwardFrom: Array[Int] = _
        private var rightForwardTo: Array[Int] = _
        private var serializer: UDTSerializer[Out] = _

        override def open(config: Configuration) = {
          super.open(config)

          this.outputRecord.setNumFields(udf.getOutputLength)

          this.leftIterator = new DeserializingIterator(udf.getLeftInputDeserializer)
          this.leftForwardFrom = udf.getLeftForwardIndexArrayFrom
          this.leftForwardTo = udf.getLeftForwardIndexArrayTo
          this.rightIterator = new DeserializingIterator(udf.getRightInputDeserializer)
          this.rightForwardFrom = udf.getRightForwardIndexArrayFrom
          this.rightForwardTo = udf.getRightForwardIndexArrayTo
          this.serializer = udf.getOutputSerializer
        }

        override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstLeftRecord = leftIterator.initialize(leftRecords)
          val firstRightRecord = rightIterator.initialize(rightRecords)
          
          if (firstRightRecord != null) {
            outputRecord.copyFrom(firstRightRecord, rightForwardFrom, rightForwardTo)
          }
          if (firstLeftRecord != null) {
            outputRecord.copyFrom(firstLeftRecord, leftForwardFrom, leftForwardTo)
          }

          val output = fun.splice.apply(leftIterator, rightIterator)

          serializer.serialize(output, outputRecord)
          out.collect(outputRecord)
        }
      }
      
      val builder = new NoKeyCoGroupBuilder(generatedStub).input1(helper.leftInput.contract).input2(helper.rightInput.contract)

      val leftKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new CoGroupContract(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = generatedStub.leftKeySelector
        override val rightKey: FieldSelector = generatedStub.rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with TwoInputHintable[LeftIn, RightIn, Out] {}
    }
    
    val result = c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
  def flatMap[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
     import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {
      val helper: CoGroupDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftKeySelection = helper.leftKeySelection
      val rightKeySelection = helper.rightKeySelection

      val generatedStub = new CoGroupStub with Serializable {
        val leftInputUDT = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
        val rightInputUDT = c.Expr[UDT[RightIn]](createUdtRightIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val leftKeySelector = new FieldSelector(leftInputUDT, leftKeySelection)
        val rightKeySelector = new FieldSelector(rightInputUDT, rightKeySelection)
        val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)
        
        private val outputRecord = new PactRecord()

        private var leftIterator: DeserializingIterator[LeftIn] = _
        private var leftForwardFrom: Array[Int] = _
        private var leftForwardTo: Array[Int] = _
        private var rightIterator: DeserializingIterator[RightIn] = _
        private var rightForwardFrom: Array[Int] = _
        private var rightForwardTo: Array[Int] = _
        private var serializer: UDTSerializer[Out] = _

        override def open(config: Configuration) = {
          super.open(config)

          this.outputRecord.setNumFields(udf.getOutputLength)

          this.leftIterator = new DeserializingIterator(udf.getLeftInputDeserializer)
          this.leftForwardFrom = udf.getLeftForwardIndexArrayFrom
          this.leftForwardTo = udf.getLeftForwardIndexArrayTo
          this.rightIterator = new DeserializingIterator(udf.getRightInputDeserializer)
          this.rightForwardFrom = udf.getRightForwardIndexArrayFrom
          this.rightForwardTo = udf.getRightForwardIndexArrayTo
          this.serializer = udf.getOutputSerializer
        }

        override def coGroup(leftRecords: JIterator[PactRecord], rightRecords: JIterator[PactRecord], out: Collector[PactRecord]) = {

          val firstLeftRecord = leftIterator.initialize(leftRecords)
          outputRecord.copyFrom(firstLeftRecord, leftForwardFrom, leftForwardTo)

          val firstRightRecord = rightIterator.initialize(rightRecords)
          outputRecord.copyFrom(firstRightRecord, rightForwardFrom, rightForwardTo)

          val output = fun.splice.apply(leftIterator, rightIterator)

          if (output.nonEmpty) {

            for (item <- output) {
              serializer.serialize(item, outputRecord)
              out.collect(outputRecord)
            }
          }
        }
      }
      
      val builder = new NoKeyCoGroupBuilder(generatedStub).input1(helper.leftInput.contract).input2(helper.rightInput.contract)

      val leftKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new CoGroupContract(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = generatedStub.leftKeySelector
        override val rightKey: FieldSelector = generatedStub.rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with TwoInputHintable[LeftIn, RightIn, Out] {}
    }
    
    val result = c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
}
