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

package eu.stratosphere.scala.operators

import language.experimental.macros
import scala.reflect.macros.Context

import eu.stratosphere.types.PactRecord
import eu.stratosphere.util.Collector
import eu.stratosphere.api.operators.Operator
import eu.stratosphere.api.record.operators.JoinOperator
import eu.stratosphere.api.record.functions.JoinFunction
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper

import eu.stratosphere.configuration.Configuration;

import java.util.{ Iterator => JIterator }

import eu.stratosphere.scala._
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.scala.codegen.{MacroContextHolder, Util}
import eu.stratosphere.scala.stubs.{JoinStubBase, JoinStub, FlatJoinStub}

class JoinDataSet[LeftIn, RightIn](val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def where[Key](keyFun: LeftIn => Key) = macro JoinMacros.whereImpl[LeftIn, RightIn, Key]
}

class JoinDataSetWithWhere[LeftIn, RightIn, Key](val leftKey: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def isEqualTo[Key](keyFun: RightIn => Key) = macro JoinMacros.isEqualToImpl[LeftIn, RightIn, Key]
}

class JoinDataSetWithWhereAndEqual[LeftIn, RightIn](val leftKey: List[Int], val rightKey: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def map[Out](fun: (LeftIn, RightIn) => Out): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro JoinMacros.map[LeftIn, RightIn, Out]
  def flatMap[Out](fun: (LeftIn, RightIn) => Iterator[Out]): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro JoinMacros.flatMap[LeftIn, RightIn, Out]
  def filter(fun: (LeftIn, RightIn) => Boolean): DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)] = macro JoinMacros.filter[LeftIn, RightIn]
}

class NoKeyMatchBuilder(s: JoinFunction) extends JoinOperator.Builder(new UserCodeObjectWrapper(s))

object JoinMacros {
  
  def whereImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSet[LeftIn, RightIn] })(keyFun: c.Expr[LeftIn => Key]): c.Expr[JoinDataSetWithWhere[LeftIn, RightIn, Key]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new JoinDataSetWithWhere[LeftIn, RightIn, Key](keySelection.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }
  
  def isEqualToImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhere[LeftIn, RightIn, Key] })(keyFun: c.Expr[RightIn => Key]): c.Expr[JoinDataSetWithWhereAndEqual[LeftIn, RightIn]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new JoinDataSetWithWhereAndEqual[LeftIn, RightIn](helper.leftKey, keySelection.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }

  def map[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Out]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[JoinStubBase[LeftIn, RightIn, Out]] = if (fun.actualType <:< weakTypeOf[JoinStub[LeftIn, RightIn, Out]])
      reify { fun.splice.asInstanceOf[JoinStubBase[LeftIn, RightIn, Out]] }
    else reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new JoinStubBase[LeftIn, RightIn, Out] {
        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {
          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          val output = fun.splice.apply(left, right)

          leftRecord.setNumFields(outputLength)
          for (field <- leftDiscard)
            leftRecord.setNull(field)

          leftRecord.copyFrom(rightRecord, rightForwardFrom, rightForwardTo)
          leftRecord.copyFrom(leftRecord, leftForwardFrom, leftForwardTo)

          serializer.serialize(output, leftRecord)
          out.collect(leftRecord)
        }
      }
    }
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val leftKeySelector = new FieldSelector(generatedStub.leftInputUDT, helper.leftKey)
      val rightKeySelector = new FieldSelector(generatedStub.rightInputUDT, helper.rightKey)

      val builder = new NoKeyMatchBuilder(generatedStub).input1(leftInput).input2(rightInput)

      val leftKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }

      
      
      val ret = new JoinOperator(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = leftKeySelector
        override val rightKey: FieldSelector = rightKeySelector
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
  
  def flatMap[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Iterator[Out]]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[JoinStubBase[LeftIn, RightIn, Out]] = if (fun.actualType <:< weakTypeOf[JoinStub[LeftIn, RightIn, Out]])
      reify { fun.splice.asInstanceOf[JoinStubBase[LeftIn, RightIn, Out]] }
    else reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new JoinStubBase[LeftIn, RightIn, Out] {
        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {
          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          val output = fun.splice.apply(left, right)

          if (output.nonEmpty) {

            leftRecord.setNumFields(outputLength)

            for (field <- leftDiscard)
              leftRecord.setNull(field)

            leftRecord.copyFrom(rightRecord, rightForwardFrom, rightForwardTo)
            leftRecord.copyFrom(leftRecord, leftForwardFrom, leftForwardTo)

            for (item <- output) {
              serializer.serialize(item, leftRecord)
              out.collect(leftRecord)
            }
          }
        }
      }
    }
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val leftKeySelector = new FieldSelector(generatedStub.leftInputUDT, helper.leftKey)
      val rightKeySelector = new FieldSelector(generatedStub.rightInputUDT, helper.rightKey)
      val builder = new NoKeyMatchBuilder(generatedStub).input1(leftInput).input2(rightInput)

      val leftKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new JoinOperator(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = leftKeySelector
        override val rightKey: FieldSelector = rightKeySelector
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
  
  def filter[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Boolean]): c.Expr[DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[(LeftIn, RightIn)]

    val stub: c.Expr[JoinStubBase[LeftIn, RightIn, (LeftIn, RightIn)]] = if (fun.actualType <:< weakTypeOf[JoinStub[LeftIn, RightIn, (LeftIn, RightIn)]])
      reify { fun.splice.asInstanceOf[JoinStubBase[LeftIn, RightIn, (LeftIn, RightIn)]] }
    else reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[(LeftIn, RightIn)] = c.Expr[UDT[(LeftIn, RightIn)]](createUdtOut).splice
      new JoinStubBase[LeftIn, RightIn, (LeftIn, RightIn)] {
        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {
          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          if (fun.splice.apply(left, right)) {
            val output = (left, right)
            leftRecord.setNumFields(outputLength)
            serializer.serialize(output, leftRecord)
            out.collect(leftRecord)
          }
        }
      }
    }
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val leftKeySelector = new FieldSelector(generatedStub.leftInputUDT, helper.leftKey)
      val rightKeySelector = new FieldSelector(generatedStub.rightInputUDT, helper.rightKey)
      val builder = new NoKeyMatchBuilder(generatedStub).input1(leftInput).input2(rightInput)

      val leftKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new JoinOperator(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, (LeftIn, RightIn)] {
        override val leftKey: FieldSelector = leftKeySelector
        override val rightKey: FieldSelector = rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[(LeftIn, RightIn)](ret) with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)] {}
    }

    val result = c.Expr[DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
}
