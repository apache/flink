/*
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
package org.apache.flink.table.plan.nodes

import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo, SimplePythonFunction}
import org.apache.flink.table.functions.utils.ScalarSqlFunction

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonCalc {

  private[flink] def extractPythonScalarFunctionInfos(
      rexCalls: Array[RexCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    // using LinkedHashMap to keep the insert order
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonFunctionInfos = rexCalls.map(createPythonScalarFunctionInfo(_, inputNodes))

    val udfInputOffsets = inputNodes.toArray.map(_._1).map {
      case inputRef: RexInputRef => inputRef.getIndex
      case _: RexLiteral => throw new Exception(
        "Constants cannot be used as parameters of Python UDF for now. " +
        "It will be supported in FLINK-14208")
    }
    (udfInputOffsets, pythonFunctionInfos)
  }

  private[flink] def createPythonScalarFunctionInfo(
      rexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer]): PythonFunctionInfo = rexCall.getOperator match {
    case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage == FunctionLanguage.PYTHON =>
      val inputs = new mutable.ArrayBuffer[AnyRef]()
      rexCall.getOperands.foreach {
        case pythonRexCall: RexCall if pythonRexCall.getOperator.asInstanceOf[ScalarSqlFunction]
          .getScalarFunction.getLanguage == FunctionLanguage.PYTHON =>
          // Continuous Python UDFs can be chained together
          val argPythonInfo = createPythonScalarFunctionInfo(pythonRexCall, inputNodes)
          inputs.append(argPythonInfo)

        case argNode: RexNode =>
          // For input arguments of RexInputRef, it's replaced with an offset into the input row
          inputNodes.get(argNode) match {
            case Some(existing) => inputs.append(existing)
            case None =>
              val inputOffset = Integer.valueOf(inputNodes.size)
              inputs.append(inputOffset)
              inputNodes.put(argNode, inputOffset)
          }
      }

      // Extracts the necessary information for Python function execution, such as
      // the serialized Python function, the Python env, etc
      val pythonFunction = new SimplePythonFunction(
        sfc.getScalarFunction.asInstanceOf[PythonFunction].getSerializedPythonFunction,
        sfc.getScalarFunction.asInstanceOf[PythonFunction].getPythonEnv)
      new PythonFunctionInfo(pythonFunction, inputs.toArray)
  }
}
