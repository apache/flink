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

import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.stubs.{Collector, MapStub => JMapStub}
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

import eu.stratosphere.scala.codegen.{MacroContextHolder, Util}
import eu.stratosphere.scala._
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.scala.stubs.{MapStub, FlatMapStub, FilterStub, MapStubBase}

object MapMacros {

  def map[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[In => Out]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[MapStubBase[In, Out]] = if (fun.actualType <:< weakTypeOf[MapStub[In, Out]])
      reify { fun.splice.asInstanceOf[MapStubBase[In, Out]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new MapStubBase[In, Out] {
        override def map(record: PactRecord, out: Collector[PactRecord]) = {
          val input = deserializer.deserializeRecyclingOn(record)
          val output = fun.splice.apply(input)

          record.setNumFields(outputLength)

          for (field <- discard)
            record.setNull(field)

          serializer.serialize(output, record)
          out.collect(record)
        }
      }
    }
    val contract = reify {
      val generatedStub = stub.splice
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, Out] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      val stream = new DataSet[Out](contract) with OneInputHintable[In, Out] {}
      contract.persistHints = { () => stream.applyHints(contract) }
      stream
    }

    val result = c.Expr[DataSet[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))

    return result
  }
  
  def flatMap[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[In => Iterator[Out]]): c.Expr[DataSet[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val stub: c.Expr[MapStubBase[In, Out]] = if (fun.actualType <:< weakTypeOf[FlatMapStub[In, Out]])
      reify { fun.splice.asInstanceOf[MapStubBase[In, Out]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice
      implicit val outputUDT: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
      new MapStubBase[In, Out] {
        override def map(record: PactRecord, out: Collector[PactRecord]) = {
          val input = deserializer.deserializeRecyclingOn(record)
          val output = fun.splice.apply(input)

          if (output.nonEmpty) {

            record.setNumFields(outputLength)

            for (field <- discard)
              record.setNull(field)

            for (item <- output) {

              serializer.serialize(item, record)
              out.collect(record)
            }
          }
        }
      }
    }
    val contract = reify {
      val generatedStub = stub.splice
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, Out] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      val stream = new DataSet[Out](contract) with OneInputHintable[In, Out] {}
      contract.persistHints = { () => stream.applyHints(contract) }
      stream
    }

    val result = c.Expr[DataSet[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))

    return result
  }
  
  def filter[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(fun: c.Expr[In => Boolean]): c.Expr[DataSet[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val stub: c.Expr[MapStubBase[In, In]] = if (fun.actualType <:< weakTypeOf[FilterStub[In, In]])
      reify { fun.splice.asInstanceOf[MapStubBase[In, In]] }
    else reify {
      implicit val inputUDT: UDT[In] = c.Expr[UDT[In]](createUdtIn).splice
      new MapStubBase[In, In] {
        override def map(record: PactRecord, out: Collector[PactRecord]) = {
          val input = deserializer.deserializeRecyclingOn(record)
          if (fun.splice.apply(input)) {
        	  out.collect(record)
          }
        }
      }
    }
    val contract = reify {
      val generatedStub = stub.splice
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, In] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFields(
            Util.filterNonForwards(getUDF.getForwardIndexArrayFrom, getUDF.getForwardIndexArrayTo)))
      }
      val stream = new DataSet[In](contract) with OneInputHintable[In, In] {}
      contract.persistHints = { () =>
        stream.applyHints(contract);
        0 until generatedStub.udf.getOutputLength foreach { i => stream.markCopied(i, i) }
      }
      stream
    }

    val result = c.Expr[DataSet[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))

    return result
  }
}
