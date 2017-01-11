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

package org.apache.flink.table.plan.rules.util

import java.util

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SqlKind, SqlOperator}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.expressions._
import org.apache.flink.table.sources.TableSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object RexProgramExpressionExtractor {

  /**
    * converts a rexProgram condition into expression
    *
    * @param rexProgram The RexProgram to analyze
    * @return converted expression
    */
  def extractExpression(rexProgram: RexProgram): Expression = {

    val refInputToName = getInputsWithNames(rexProgram)
    val visitor = new ExpressionVisitor(refInputToName)

    val condition = rexProgram.getCondition
    if (condition == null) {
      return null
    }

    rexProgram.expandLocalRef(condition).accept(visitor)
    val parsedExpression = ExpressionParser.parseExpression(visitor.getStringPredicate)

    parsedExpression
  }

  /**
    * verify can the original expression be divided into `new` expression
    * and remainder part without loss of logical correctness
    *
    * @param original initial expression
    * @param lump part of original expression
    * @return whether or not to decouple parts of the origin expression
    */
  def verifyExpressions(original: Expression, lump: Expression): Boolean = {
    if (original == null & lump == null) {
      return false
    }
    if (original.children.isEmpty | !checkOperator(original)) {
      return false
    }
    val head = original.children.head
    val last = original.children.last
    if (head.checkEquals(lump)) {
      return checkOperator(original)
    }
    if (last.checkEquals(lump)) {
      return checkOperator(original)
    }
    verifyExpressions(head, lump) match {
      case true => true
      case _ => verifyExpressions(last, lump)
    }
  }

  private def checkOperator(original: Expression): Boolean = {
    original match {
      case o: Or => false
      case _ => true
    }
  }

  /**
    * Generates a new RexProgram based on new expression.
    *
    * @param rexProgram original RexProgram
    * @param scan input source
    * @param expression filter condition (fields must be resolved)
    * @param tableSource source to get names and type of table
    * @param relBuilder builder for converting expression to Rex
    */
  def rewriteRexProgram(
      rexProgram: RexProgram,
      scan: TableScan,
      expression: Expression,
      tableSource: TableSource[_])(implicit relBuilder: RelBuilder): RexProgram = {

    if (expression != null) {

      val names = TableEnvironment.getFieldNames(tableSource)

      val nameToType = names
        .zip(TableEnvironment.getFieldTypes(tableSource)).toMap

      relBuilder.push(scan)

      val rule: PartialFunction[Expression, Expression] = {
        case u@UnresolvedFieldReference(name) =>
          ResolvedFieldReference(name, nameToType(name))
      }

      val newProjectExpressions = rewriteProjects(rexProgram, names)

      val resolvedExp = expression.postOrderTransform(rule)

      RexProgram.create(
        rexProgram.getInputRowType,
        newProjectExpressions,
        resolvedExp.toRexNode,
        rexProgram.getOutputRowType,
        relBuilder.getRexBuilder)
    } else {
      rexProgram
    }
  }

  private def rewriteProjects(
      rexProgram: RexProgram,
      names: Array[String]): util.List[RexNode] = {

    val inputRewriter = new InputRewriter(names.indices.toArray)
    val newProject = rexProgram.getProjectList.map(
      exp => rexProgram.expandLocalRef(exp).accept(inputRewriter)
    ).toList.asJava
    newProject
  }

  private def getInputsWithNames(rexProgram: RexProgram): Map[RexInputRef, String] = {
    val names = rexProgram.getInputRowType.getFieldNames
    rexProgram.getExprList.asScala.map {
      case i: RexInputRef =>
        i -> names(i.getIndex)
      case _ => null
    }.filter(_ != null)
      .toMap
  }
}

/**
  * A RexVisitor to extract expression of condition
  */
class ExpressionVisitor(
    nameMap: Map[RexInputRef, String])
  extends RexVisitorImpl[Unit](true) {

  private val predicateBuilder = new mutable.StringBuilder()

  def getStringPredicate: String = predicateBuilder.toString()

  override def visitInputRef(inputRef: RexInputRef): Unit = {
    predicateBuilder.append(nameMap(inputRef))
  }

  override def visitLiteral(literal: RexLiteral): Unit = {
    predicateBuilder.append(literal.getValue)
  }

  override def visitLocalRef(localRef: RexLocalRef): Unit = {
    localRef.accept(this)
  }

  override def visitCall(call: RexCall): Unit = {
    call.operands(0).accept(this)
    val op = getTableLikeOperator(call.op)
    predicateBuilder.append(s" $op ")
    call.operands(1).accept(this)
  }

  def getTableLikeOperator(op: SqlOperator) = {
    op.getKind match {
      case SqlKind.AND => "&&"
      case SqlKind.OR => "||"
      case _ => op.toString
    }
  }
}
