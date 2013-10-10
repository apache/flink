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
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.scala.DataStream
import eu.stratosphere.scala.OneInputHintable

object MapMacros {

  def map[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataStream[In] })(fun: c.Expr[In => Out]): c.Expr[DataStream[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val contract = reify {

      val generatedStub = new MapStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)
        
        private var deserializer: UDTSerializer[In] = _
        private var serializer: UDTSerializer[Out] = _
        private var discard: Array[Int] = _
        private var outputLength: Int = _

        override def open(config: Configuration) {
          super.open(config)

          discard = udf.getDiscardIndexArray
          outputLength = udf.getOutputLength

          this.deserializer = udf.getInputDeserializer
          this.serializer = udf.getOutputSerializer
        }

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
      
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, Out] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      val stream = new DataStream[Out](contract) with OneInputHintable[In, Out] {}
      contract.persistHints = { () => stream.applyHints(contract) }
      stream
    }

    val result = c.Expr[DataStream[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))

    return result
  }
  
  def flatMap[In: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = DataStream[In] })(fun: c.Expr[In => Iterator[Out]]): c.Expr[DataStream[Out] with OneInputHintable[In, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]

    val contract = reify {

      val generatedStub = new MapStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)
        
        private var deserializer: UDTSerializer[In] = _
        private var serializer: UDTSerializer[Out] = _
        private var discard: Array[Int] = _
        private var outputLength: Int = _

        override def open(config: Configuration) {
          super.open(config)

          discard = udf.getDiscardIndexArray
          outputLength = udf.getOutputLength

          this.deserializer = udf.getInputDeserializer
          this.serializer = udf.getOutputSerializer
        }

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
      
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, Out] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      val stream = new DataStream[Out](contract) with OneInputHintable[In, Out] {}
      contract.persistHints = { () => stream.applyHints(contract) }
      stream
    }

    val result = c.Expr[DataStream[Out] with OneInputHintable[In, Out]](Block(List(udtIn, udtOut), contract.tree))

    return result
  }
  
  def filter[In: c.WeakTypeTag](c: Context { type PrefixType = DataStream[In] })(fun: c.Expr[In => Boolean]): c.Expr[DataStream[In] with OneInputHintable[In, In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val contract = reify {

      val generatedStub = new MapStub with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val outputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val udf: UDF1[In, In] = new UDF1(inputUDT, outputUDT)
        
        private var deserializer: UDTSerializer[In] = _
//        private var serializer: UDTSerializer[In] = _
        private var discard: Array[Int] = _
        private var outputLength: Int = _

        override def open(config: Configuration) {
          super.open(config)

          discard = udf.getDiscardIndexArray
          outputLength = udf.getOutputLength

          this.deserializer = udf.getInputDeserializer
//          this.serializer = udf.getOutputSerializer
        }

        override def map(record: PactRecord, out: Collector[PactRecord]) = {
          val input = deserializer.deserializeRecyclingOn(record)
          if (fun.splice.apply(input)) {
        	  out.collect(record)
          }
        }
      }
      
      val builder = MapContract.builder(generatedStub).input((c.prefix.splice).contract)
      
      val contract = new MapContract(builder) with OneInputScalaContract[In, In] {
        override def getUDF = generatedStub.udf
        override def annotations = Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      }
      val stream = new DataStream[In](contract) with OneInputHintable[In, In] {}
      contract.persistHints = { () =>
        stream.applyHints(contract);
        0 until generatedStub.udf.getOutputLength foreach { i => stream.markCopied(i, i) }
      }
      stream
    }

    val result = c.Expr[DataStream[In] with OneInputHintable[In, In]](Block(List(udtIn), contract.tree))

    return result
  }
}
