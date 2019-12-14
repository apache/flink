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
package org.apache.flink.table.plan.util

import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.functions.python.PythonFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction

import scala.collection.JavaConversions._

object PythonUtil {

  /**
    * Checks whether it contains Python function call in the specified node.
    *
    * @param node the RexNode to check
    * @return true if it contains the Python function call in the specified node.
    */
  def containsPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(true, true))

  /**
    * Checks whether it contains non-Python function call in the specified node.
    *
    * @param node the RexNode to check
    * @return true if it contains the non-Python function call in the specified node.
    */
  def containsNonPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(false, true))

  /**
    * Checks whether the specified node is a Python function call.
    *
    * @param node the RexNode to check
    * @return true if the specified node is a Python function call.
    */
  def isPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(true, false))

  /**
    * Checks whether the specified node is a non-Python function call.
    *
    * @param node the RexNode to check
    * @return true if the specified node is a non-Python function call.
    */
  def isNonPythonCall(node: RexNode): Boolean = node.accept(new FunctionFinder(false, false))

  /**
    * Checks whether it contains the specified kind of function in a RexNode.
    *
    * @param findPythonFunction true to find python function, false to find non-python function
    * @param recursive whether check the inputs
    */
  private class FunctionFinder(findPythonFunction: Boolean, recursive: Boolean)
    extends RexDefaultVisitor[Boolean] {

    /**
      * Checks whether the specified rexCall is python function call.
      *
      * @param rexCall the RexCall to check.
      * @return true if it is python function call.
      */
    private def isPythonRexCall(rexCall: RexCall): Boolean = rexCall.getOperator match {
      case sfc: ScalarSqlFunction => sfc.getScalarFunction.isInstanceOf[PythonFunction]
      case _ => false
    }

    override def visitCall(call: RexCall): Boolean = {
      findPythonFunction == isPythonRexCall(call) ||
        (recursive && call.getOperands.exists(_.accept(this)))
    }

    override def visitNode(rexNode: RexNode): Boolean = false
  }
}
