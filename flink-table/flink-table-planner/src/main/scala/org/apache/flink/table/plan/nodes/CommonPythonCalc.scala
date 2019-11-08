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
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo, SimplePythonFunction}
import org.apache.flink.table.functions.utils.ScalarSqlFunction

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonCalc {

  protected def loadClass(className: String): Class[_] = {
    try {
      Class.forName(className, false, Thread.currentThread.getContextClassLoader)
    } catch {
      case ex: ClassNotFoundException => throw new TableException(
        "The dependency of 'flink-python' is not present on the classpath.", ex)
    }
  }

  private lazy val convertLiteralToPython = {
    val clazz = loadClass("org.apache.flink.api.common.python.PythonBridgeUtils")
    clazz.getMethod("convertLiteralToPython", classOf[RexLiteral], classOf[SqlTypeName])
  }

  private[flink] def extractPythonScalarFunctionInfos(
      pythonRexCalls: Array[RexCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    // using LinkedHashMap to keep the insert order
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonFunctionInfos = pythonRexCalls.map(createPythonScalarFunctionInfo(_, inputNodes))

    val udfInputOffsets = inputNodes.toArray
      .map(_._1)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
    (udfInputOffsets, pythonFunctionInfos)
  }

  private[flink] def createPythonScalarFunctionInfo(
      pythonRexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer]): PythonFunctionInfo = {
    pythonRexCall.getOperator match {
      case sfc: ScalarSqlFunction =>
        val inputs = new mutable.ArrayBuffer[AnyRef]()
        pythonRexCall.getOperands.foreach {
          case pythonRexCall: RexCall =>
            // Continuous Python UDFs can be chained together
            val argPythonInfo = createPythonScalarFunctionInfo(pythonRexCall, inputNodes)
            inputs.append(argPythonInfo)

          case literal: RexLiteral =>
            inputs.append(
              convertLiteralToPython.invoke(null, literal, literal.getType.getSqlTypeName))

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
}
