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

import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.flink.table.functions.python.PythonFunctionInfo

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonCalc extends CommonPythonBase {

  private[flink] def extractPythonScalarFunctionInfos(
      pythonRexCalls: Array[RexCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    // using LinkedHashMap to keep the insert order
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonFunctionInfos = pythonRexCalls.map(createPythonFunctionInfo(_, inputNodes))

    val udfInputOffsets = inputNodes.toArray
      .map(_._1)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
    (udfInputOffsets, pythonFunctionInfos)
  }

  private[flink] def getPythonRexCalls(calcProgram: RexProgram): Array[RexCall] = {
    calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .collect { case call: RexCall => call }
      .toArray
  }

  private[flink] def getForwardedFields(calcProgram: RexProgram): Array[Int] = {
    calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
      .toArray
  }
}
