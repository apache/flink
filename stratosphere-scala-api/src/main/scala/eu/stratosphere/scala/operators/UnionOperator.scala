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
import scala.collection.JavaConversions._
import scala.reflect.macros.Context
import eu.stratosphere.scala.codegen.MacroContextHolder
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.api.record.operators.MapOperator
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.types.Record
import eu.stratosphere.api.record.functions.MapFunction
import eu.stratosphere.util.Collector
import eu.stratosphere.api.operators.Operator
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.scala.UnionScalaContract
import eu.stratosphere.scala.DataSet

object UnionMacros {

  def impl[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(secondInput: c.Expr[DataSet[In]]): c.Expr[DataSet[In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val contract = reify {

      val generatedStub = new MapFunction with Serializable {
        val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        val udf: UDF1[In, In] = new UDF1(inputUDT, inputUDT)

        override def map(record: Record, out: Collector[Record]) = out.collect(record)
      }

      val firstInputs = c.prefix.splice.contract match {
        case c : MapOperator with UnionScalaContract[_] => c.getInputs().toList
        case c => List(c)
      }

      val secondInputs = secondInput.splice.contract match {
        case c : MapOperator with UnionScalaContract[_] => c.getInputs().toList
        case c => List(c)
      }

      val builder = MapOperator.builder(generatedStub).inputs(firstInputs ++ secondInputs)
      
      val ret = new MapOperator(builder) with UnionScalaContract[In] {
        override def getUDF = generatedStub.udf
      }
      new DataSet(ret)
    }

    val result = c.Expr[DataSet[In]](Block(List(udtIn), contract.tree))

    return result
  }
}
