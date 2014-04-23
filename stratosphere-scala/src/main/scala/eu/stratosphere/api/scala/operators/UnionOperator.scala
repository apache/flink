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
import scala.collection.JavaConversions._
import scala.reflect.macros.Context
import eu.stratosphere.api.scala.codegen.MacroContextHolder
import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.java.record.operators.MapOperator
import eu.stratosphere.api.scala.analysis.UDT
import eu.stratosphere.types.Record
import eu.stratosphere.api.java.record.functions.MapFunction
import eu.stratosphere.util.Collector
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.analysis.UDF1
import eu.stratosphere.api.scala.analysis.UDTSerializer
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.scala.analysis.UDF0
import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.scala.UnionScalaOperator
import eu.stratosphere.api.scala.DataSet
import eu.stratosphere.api.scala.analysis.UDF2
import eu.stratosphere.api.common.operators.Union

object UnionMacros {

  def impl[In: c.WeakTypeTag](c: Context { type PrefixType = DataSet[In] })(secondInput: c.Expr[DataSet[In]]): c.Expr[DataSet[In]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
//    val (paramName, udfBody) = slave.extractOneInputUdf(fun.tree)

    val (udtIn, createUdtIn) = slave.mkUdtClass[In]

    val contract = reify {

      val firstInOp = c.prefix.splice.contract;
      val secondInOp = secondInput.splice.contract
      
      val ret = new Union(firstInOp, secondInOp) with UnionScalaOperator[In] {
        private val inputUDT = c.Expr[UDT[In]](createUdtIn).splice
        private val udf: UDF2[In, In, In] = new UDF2(inputUDT, inputUDT, inputUDT)
        
        override def getUDF = udf;
      }
      new DataSet(ret)
    }

    val result = c.Expr[DataSet[In]](Block(List(udtIn), contract.tree))

    return result
  }
}
