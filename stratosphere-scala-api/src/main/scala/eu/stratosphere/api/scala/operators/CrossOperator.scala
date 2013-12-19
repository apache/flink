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

package eu.stratosphere.api.scala.operators

import language.experimental.macros
import scala.reflect.macros.Context

import eu.stratosphere.api.record.operators.MapOperator

import eu.stratosphere.types.Record
import eu.stratosphere.util.Collector
import eu.stratosphere.api.operators.Operator
import eu.stratosphere.api.record.operators.CrossOperator

import eu.stratosphere.configuration.Configuration;
import java.util.{ Iterator => JIterator }

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.analysis._
import eu.stratosphere.api.scala.functions.{CrossFunctionBase, CrossFunction, FlatCrossFunction}
import eu.stratosphere.api.scala.codegen.{MacroContextHolder, Util}
import eu.stratosphere.api.scala.functions.DeserializingIterator

class CrossDataSet[LeftIn, RightIn](val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def map[Out](fun: (LeftIn, RightIn) => Out): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro CrossMacros.map[LeftIn, RightIn, Out]
  def flatMap[Out](fun: (LeftIn, RightIn) => Iterator[Out]): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro CrossMacros.flatMap[LeftIn, RightIn, Out]
  def filter(fun: (LeftIn, RightIn) => Boolean): DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)] = macro CrossMacros.filter[LeftIn, RightIn]
}

object CrossMacros {

  def map[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = CrossDataSet[LeftIn, RightIn] })
                                                                            (fun: c.Expr[(LeftIn, RightIn) => Out]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[CrossFunctionBase[LeftIn, RightIn, Out]] = if (fun.actualType <:< weakTypeOf[CrossFunction[LeftIn, RightIn, Out]])
      reify { fun.splice.asInstanceOf[CrossFunctionBase[LeftIn, RightIn, Out]] }
    else reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new CrossFunctionBase[LeftIn, RightIn, Out] {
        override def cross(leftRecord: Record, rightRecord: Record, out: Collector[Record]) = {
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
      val helper: CrossDataSet[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val builder = CrossOperator.builder(generatedStub).input1(leftInput).input2(rightInput)
      
      val ret = new CrossOperator(builder) with TwoInputScalaOperator[LeftIn, RightIn, Out] {
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
  
  def flatMap[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = CrossDataSet[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Iterator[Out]]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[CrossFunctionBase[LeftIn, RightIn, Out]] = if (fun.actualType <:< weakTypeOf[CrossFunction[LeftIn, RightIn, Out]])
      reify { fun.splice.asInstanceOf[CrossFunctionBase[LeftIn, RightIn, Out]] }
    else reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new CrossFunctionBase[LeftIn, RightIn, Out] {
        override def cross(leftRecord: Record, rightRecord: Record, out: Collector[Record]) = {
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
      val helper: CrossDataSet[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val builder = CrossOperator.builder(generatedStub).input1(leftInput).input2(rightInput)
      
      val ret = new CrossOperator(builder) with TwoInputScalaOperator[LeftIn, RightIn, Out] {
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

  def filter[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag](c: Context { type PrefixType = CrossDataSet[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Boolean]): c.Expr[DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)

    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[(LeftIn, RightIn)]

    val stub = reify {
      implicit val leftInputUDT: UDT[LeftIn] = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
      implicit val rightInputUDT: UDT[RightIn] = c.Expr[UDT[RightIn]](createUdtRightIn).splice
      implicit val outputUDT: UDT[(LeftIn, RightIn)] = c.Expr[UDT[(LeftIn, RightIn)]](createUdtOut).splice
      new CrossFunctionBase[LeftIn, RightIn, (LeftIn, RightIn)] {
        override def cross(leftRecord: Record, rightRecord: Record, out: Collector[Record]) = {
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
      val helper: CrossDataSet[LeftIn, RightIn] = c.prefix.splice
      val leftInput = helper.leftInput.contract
      val rightInput = helper.rightInput.contract
      val generatedStub = ClosureCleaner.clean(stub.splice)
      val builder = CrossOperator.builder(generatedStub).input1(leftInput).input2(rightInput)
      
      val ret = new CrossOperator(builder) with TwoInputScalaOperator[LeftIn, RightIn, (LeftIn, RightIn)] {
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
