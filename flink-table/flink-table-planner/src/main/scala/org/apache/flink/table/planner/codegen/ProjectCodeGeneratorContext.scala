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

package org.apache.flink.table.planner.codegen

import java.util

import org.apache.calcite.rex.{RexLocalRef, RexNode}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

import scala.collection.mutable

/**
 * The context for code generator, maintaining reusable common expressions.
 */
class ProjectCodeGeneratorContext(
   tableConfig: TableConfig,
   expList: util.List[RexNode],
   localRef: Seq[RexNode],
   projection: Seq[RexNode])
  extends CodeGeneratorContext(tableConfig) {

  private val localRefs = localRef.map(v => v.asInstanceOf[RexLocalRef])
  private val projectionIndexMapping: mutable.Map[Int, RexNode] =
    mutable.Map[Int, RexNode]()
  JavaScalaConversionUtil.toScala(expList).zipWithIndex.foreach {
    case(rexNode, index)=>
      projectionIndexMapping.put(index, rexNode)
  }

  private val reuseExpression: mutable.Map[RexNode, GeneratedExpression] =
    mutable.Map[RexNode, GeneratedExpression]()

  def addCommonExpression(rexNode: RexNode, expression: GeneratedExpression):
  GeneratedExpression = {
    reuseExpression.put(rexNode, expression)
    expression
  }

  def cleanupCommonExpression() = {
    reuseExpression.clear()
  }

  def getCommonExpression(rexNode: RexNode): GeneratedExpression = {
    reuseExpression.getOrElse(rexNode, null)
  }

  def commonExpressionElimination(fieldExprs: Seq[GeneratedExpression]):
    Seq[GeneratedExpression] = {
      if(localRefs == null) {
        return fieldExprs
      }
      var result:Seq[GeneratedExpression] = List()
      var offset = 0
      localRefs.foreach(localRef => {
        var code = ""
        for(i <- offset until localRef.getIndex + 1) {
          val expression = fieldExprs(i)
          code =
            s"""$code
               |${expression.code}""".stripMargin
        }
        result = result :+  fieldExprs(localRef.getIndex).copy(code = code)
        offset = localRef.getIndex + 1
      })
      result
  }

  def getLocalRefs = localRefs

  def getProjection = projection

  def getRexNodeIndex(index: Int) : RexNode = {
    projectionIndexMapping.getOrElse(index, null)
  }
}
