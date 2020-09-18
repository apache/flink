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

package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.functions.FunctionDefinition
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionKind}
import org.apache.flink.table.planner.functions.bridging.{BridgingSqlAggFunction, BridgingSqlFunction}
import org.apache.flink.table.planner.functions.utils.{AggSqlFunction, ScalarSqlFunction, TableSqlFunction}

import scala.collection.JavaConversions._

object PythonUtil {

  /**
    * Checks whether it contains the specified kind of Python function call in the specified node.
    * If the parameter pythonFunctionKind is null, it will return true for any kind of Python
    * function.
    *
    * @param node the RexNode to check
    * @param pythonFunctionKind the kind of the python function
    * @return true if it contains the Python function call in the specified node.
    */
  def containsPythonCall(node: RexNode, pythonFunctionKind: PythonFunctionKind = null): Boolean =
    node.accept(new FunctionFinder(true, Option(pythonFunctionKind), true))

  /**
    * Checks whether it contains non-Python function call in the specified node.
    *
    * @param node the RexNode to check
    * @return true if it contains the non-Python function call in the specified node.
    */
  def containsNonPythonCall(node: RexNode): Boolean =
    node.accept(new FunctionFinder(false, None, true))

  /**
    * Checks whether the specified node is the specified kind of Python function call.
    * If the parameter pythonFunctionKind is null, it will return true for any kind of Python
    * function.
    *
    * @param node the RexNode to check
    * @param pythonFunctionKind the kind of the python function
    * @return true if the specified node is a Python function call.
    */
  def isPythonCall(node: RexNode, pythonFunctionKind: PythonFunctionKind = null): Boolean =
    node.accept(new FunctionFinder(true, Option(pythonFunctionKind), false))

  /**
    * Checks whether the specified node is a non-Python function call.
    *
    * @param node the RexNode to check
    * @return true if the specified node is a non-Python function call.
    */
  def isNonPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(false, None, false))

  /**
    * Checks whether the specified aggregate is the specified kind of Python function Aggregate.
    *
    * @param call the AggregateCall to check
    * @param pythonFunctionKind the kind of the python function
    * @return true if the specified call is a Python function Aggregate.
    */
  def isPythonAggregate(
      call: AggregateCall,
      pythonFunctionKind: PythonFunctionKind = null): Boolean = {
    val aggregation = call.getAggregation
    aggregation match {
      case function: AggSqlFunction =>
        isPythonFunction(function.aggregateFunction, pythonFunctionKind)
      case function: BridgingSqlAggFunction =>
        isPythonFunction(function.getDefinition, pythonFunctionKind)
      case _ => false
    }
  }

  private[this] def isPythonFunction(
      function: FunctionDefinition,
      pythonFunctionKind: PythonFunctionKind): Boolean = {
    function match {
      case pythonFunction: PythonFunction =>
        pythonFunctionKind == null || pythonFunction.getPythonFunctionKind == pythonFunctionKind
      case _ => false
    }
  }

  /**
    * Checks whether it contains the specified kind of function in a RexNode.
    *
    * @param findPythonFunction true to find python function, false to find non-python function
    * @param pythonFunctionKind the kind of the python function
    * @param recursive whether check the inputs
    */
  private class FunctionFinder(
      findPythonFunction: Boolean,
      pythonFunctionKind: Option[PythonFunctionKind],
      recursive: Boolean)
    extends RexDefaultVisitor[Boolean] {

    /**
      * Checks whether the specified rexCall is a python function call of the specified kind.
      *
      * @param rexCall the RexCall to check.
      * @return true if it is python function call of the specified kind.
      */
    private def isPythonRexCall(rexCall: RexCall): Boolean =
      rexCall.getOperator match {
        case sfc: ScalarSqlFunction => isPythonFunction(sfc.scalarFunction)
        case tfc: TableSqlFunction => isPythonFunction(tfc.udtf)
        case bsf: BridgingSqlFunction => isPythonFunction(bsf.getDefinition)
        case _ => false
    }

    private def isPythonFunction(functionDefinition: FunctionDefinition): Boolean = {
      functionDefinition.isInstanceOf[PythonFunction] &&
        (pythonFunctionKind.isEmpty ||
          functionDefinition.asInstanceOf[PythonFunction].getPythonFunctionKind ==
            pythonFunctionKind.get)
    }

    override def visitCall(call: RexCall): Boolean = {
      findPythonFunction == isPythonRexCall(call) ||
        (recursive && call.getOperands.exists(_.accept(this)))
    }

    override def visitNode(rexNode: RexNode): Boolean = false
  }
}
