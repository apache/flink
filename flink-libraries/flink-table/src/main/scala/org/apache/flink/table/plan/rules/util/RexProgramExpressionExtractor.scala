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

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SqlKind, SqlOperator}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._

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
  def extractPredicateExpression(
      rexProgram: RexProgram,
      rexBuilder: RexBuilder): Option[Array[Expression]] = {

    val refInputToName = getInputsWithNames(rexProgram)
    val visitor = new ExpressionVisitor(refInputToName)

    val condition = rexProgram.getCondition
    if (condition == null) {
      return None
    }

    val cnf: RexNode = RexUtil.toCnf(rexBuilder, condition)
    cnf match {
      case ref: RexLocalRef =>
        rexProgram.expandLocalRef(ref).accept(visitor)
      case _ =>
        throw new TableException("cnf node is not RexLocalRef")
    }

    cnf.accept(visitor)
    Option(visitor.getCNFPredicates)
  }

  /**
    * verify should we apply remained expressions on
    *
    * @param original initial expression
    * @param remained remained part of original expression
    * @return whether or not to decouple parts of the origin expression
    */
  def verifyExpressions(
      original: Array[Expression],
      remained: Array[Expression]): Boolean =
    remained forall (original contains)

  /**
    * Generates a new RexProgram based on new expression.
    *
    * @param rexProgram original RexProgram
    * @param scan input source
    * @param predicate filter condition (fields must be resolved)
    * @param relBuilder builder for converting expression to Rex
    */
  def rewriteRexProgram(
      rexProgram: RexProgram,
      scan: TableScan,
      predicate: Array[Expression])(implicit relBuilder: RelBuilder): RexProgram = {

    val inType = rexProgram.getInputRowType
    val fieldTypes: Map[String, TypeInformation[_]] = inType.getFieldList
      .map(f => f.getName -> FlinkTypeFactory.toTypeInfo(f.getType))
      .toMap

    relBuilder.push(scan)

    val rule: PartialFunction[Expression, Expression] = {
      case u@UnresolvedFieldReference(name) =>
        ResolvedFieldReference(name, fieldTypes(name))
    }

    val projs = rexProgram.getProjectList.map(rexProgram.expandLocalRef)
    val resolvedExp = predicate.map(_.postOrderTransform(rule))
    val expr: Expression = ExpressionParser.parseExpression(resolvedExp mkString "&&")

    RexProgram.create(
      inType,
      projs,
      expr.toRexNode,
      rexProgram.getOutputRowType,
      relBuilder.getRexBuilder)
  }

  private def getInputsWithNames(rexProgram: RexProgram): Map[RexInputRef, String] = {
    val names = rexProgram.getInputRowType.getFieldNames

    val buffer = for {
      exp <- rexProgram.getExprList.asScala
      if exp.isInstanceOf[RexInputRef]
      ref = exp.asInstanceOf[RexInputRef]
    } yield {
      ref -> names(ref.getIndex)
    }
    buffer.toMap
  }
}

/**
  * A RexVisitor to extract expression of condition
  */
class ExpressionVisitor(
    nameMap: Map[RexInputRef, String])
  extends RexVisitorImpl[Expression](true) {

  private var predicateCNFArray = new mutable.ArrayBuffer[Expression]()

  def getCNFPredicates: Array[Expression] = predicateCNFArray.toArray

  override def visitInputRef(inputRef: RexInputRef): Expression = {
    predicateCNFArray += UnresolvedFieldReference("stub")
    UnresolvedFieldReference("stub")
  }

  override def visitLiteral(literal: RexLiteral): Expression = {
    null
  }

  override def visitLocalRef(localRef: RexLocalRef): Expression = {
    null
  }

  override def visitCall(call: RexCall): Expression = {
    call.operands(0).accept(this)
    val op = getTableLikeOperator(call.op)
    call.operands(1).accept(this)
  }
}
