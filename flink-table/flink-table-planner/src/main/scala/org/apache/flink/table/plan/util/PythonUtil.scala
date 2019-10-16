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
    * Checks whether it contains the specified kind of function in the specified node.
    *
    * @param node the RexNode to check
    * @param findPythonFunction true to find python function, false to find non-python function
    * @param recursive whether check the inputs of the specified node
    * @return true if it contains the specified kind of function in the specified node.
    */
  def containsFunctionOf(
      node: RexNode,
      findPythonFunction: Boolean,
      recursive: Boolean = true): Boolean = {
    node.accept(new FunctionFinder(findPythonFunction, recursive))
  }

  /**
    * Checks whether the specified rexCall is python function call.
    *
    * @param rexCall the RexCall to check.
    * @return true if it is python function call.
    */
  def isPythonCall(rexCall: RexCall): Boolean = rexCall.getOperator match {
    case sfc: ScalarSqlFunction => sfc.getScalarFunction.isInstanceOf[PythonFunction]
    case _ => false
  }

  /**
    * Checks whether it contains the specified kind of function in a RexNode.
    *
    * @param findPythonFunction true to find python function, false to find java function
    * @param recursive whether check the inputs
    */
  private class FunctionFinder(findPythonFunction: Boolean, recursive: Boolean)
    extends RexDefaultVisitor[Boolean] {

    override def visitCall(call: RexCall): Boolean = {
      findPythonFunction == isPythonCall(call) ||
        (recursive && call.getOperands.exists(_.accept(this)))
    }

    override def visitNode(rexNode: RexNode): Boolean = false
  }
}
