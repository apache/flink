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
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.utils.ScalarSqlFunction

import scala.collection.JavaConversions._

object PythonUtil {

  /**
    * Checks whether it contains the specified kind of function in the specified node.
    *
    * @param node the RexNode to check
    * @param language the expected kind of function to find
    * @param recursive whether check the inputs of the specified node
    * @return true if it contains the specified kind of function in the specified node.
    */
  def containsFunctionOf(
      node: RexNode,
      language: FunctionLanguage,
      recursive: Boolean = true): Boolean = {
    node.accept(new FunctionFinder(language, recursive))
  }

  /**
    * Checks whether it contains the specified kind of function in a RexNode.
    *
    * @param expectedLanguage the expected kind of function to find
    * @param recursive whether check the inputs
    */
  private class FunctionFinder(expectedLanguage: FunctionLanguage, recursive: Boolean)
    extends RexDefaultVisitor[Boolean] {

    override def visitCall(call: RexCall): Boolean = {
      call.getOperator match {
        case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
          FunctionLanguage.PYTHON =>
          findInternal(FunctionLanguage.PYTHON, call)
        case _ =>
          findInternal(FunctionLanguage.JVM, call)
      }
    }

    override def visitNode(rexNode: RexNode): Boolean = false

    private def findInternal(actualLanguage: FunctionLanguage, call: RexCall): Boolean =
      actualLanguage == expectedLanguage ||
        (recursive && call.getOperands.exists(_.accept(this)))
  }
}
