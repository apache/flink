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

import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.functions.utils.{ScalarSqlFunction, TableSqlFunction}

import scala.collection.mutable
import scala.collection.JavaConversions._

trait CommonPythonBase {
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

  private def createPythonFunctionInfo(
      pythonRexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer],
      func: UserDefinedFunction): PythonFunctionInfo = {
    val inputs = new mutable.ArrayBuffer[AnyRef]()
    pythonRexCall.getOperands.foreach {
      case pythonRexCall: RexCall =>
        // Continuous Python UDFs can be chained together
        val argPythonInfo = createPythonFunctionInfo(pythonRexCall, inputNodes)
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

    new PythonFunctionInfo(func.asInstanceOf[PythonFunction], inputs.toArray)
  }

  protected def createPythonFunctionInfo(
      pythonRexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer]): PythonFunctionInfo = {
    pythonRexCall.getOperator match {
      case sfc: ScalarSqlFunction =>
        createPythonFunctionInfo(pythonRexCall, inputNodes, sfc.getScalarFunction)
      case tfc: TableSqlFunction =>
        createPythonFunctionInfo(pythonRexCall, inputNodes, tfc.getTableFunction)
    }
  }

  protected def getConfig(tableConfig: TableConfig): Configuration = {
    val config = new Configuration(tableConfig.getConfiguration)
    config.setString("table.exec.timezone", tableConfig.getLocalTimeZone.getId)
    config
  }
}
