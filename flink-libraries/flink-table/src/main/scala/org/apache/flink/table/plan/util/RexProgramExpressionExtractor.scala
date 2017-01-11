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

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex._
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.{FlinkTypeFactory, RexNodeWrapper}
import org.apache.flink.table.expressions._
import org.apache.flink.table.validate.FunctionCatalog

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq

object RexProgramExpressionExtractor {

  /**
    * converts a rexProgram condition into independent CNF expressions
    *
    * @param rexProgram The RexProgram to analyze
    * @return converted expression
    */
  private[flink] def extractPredicateExpressions(
      rexProgram: RexProgram,
      rexBuilder: RexBuilder,
      catalog: FunctionCatalog): Array[Expression] = {

    val fieldNames = getInputsWithNames(rexProgram)

    val condition = rexProgram.getCondition
    if (condition == null) {
      return Array.empty
    }
    val call = rexProgram.expandLocalRef(condition)
    val cnf = RexUtil.toCnf(rexBuilder, call)
    val conjunctions = RelOptUtil.conjunctions(cnf)
    val expressions = conjunctions.asScala.map(
      RexNodeWrapper.wrap(_, catalog).toExpression(fieldNames)
    )
    expressions.toArray
  }

  /**
    * verify should we apply remained expressions on
    *
    * @param original initial expression
    * @param remained remained part of original expression
    * @return whether or not to decouple parts of the origin expression
    */
  private[flink] def verifyExpressions(
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
  private[flink] def rewriteRexProgram(
      rexProgram: RexProgram,
      scan: TableScan,
      predicate: Array[Expression])(implicit relBuilder: RelBuilder): RexProgram = {

    relBuilder.push(scan)

    val inType = rexProgram.getInputRowType
    val resolvedExps = resolveFields(predicate, inType)
    val projs = rexProgram.getProjectList.map(rexProgram.expandLocalRef)

    RexProgram.create(
      inType,
      projs,
      conjunct(resolvedExps).get.toRexNode,
      rexProgram.getOutputRowType,
      relBuilder.getRexBuilder)
  }

  private[flink] def getFilterExpressionAsRexNode(
      inputTpe: RelDataType,
      scan: TableScan,
      exps: Array[Expression])(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.push(scan)
    val resolvedExps = resolveFields(exps, inputTpe)
    val fullExp = conjunct(resolvedExps)
    if (fullExp.isDefined) {
      fullExp.get.toRexNode
    } else {
      null
    }
  }

  private def resolveFields(
      predicate: Array[Expression],
      inType: RelDataType): Array[Expression] = {
    val fieldTypes: Map[String, TypeInformation[_]] = inType.getFieldList
      .map(f => f.getName -> FlinkTypeFactory.toTypeInfo(f.getType))
      .toMap
    val rule: PartialFunction[Expression, Expression] = {
      case u@UnresolvedFieldReference(name) =>
        ResolvedFieldReference(name, fieldTypes(name))
    }
    predicate.map(_.postOrderTransform(rule))
  }

  private def conjunct(exps: Array[Expression]): Option[Expression] = {
    def overIndexes(): IndexedSeq[Expression] = {
      for {
        i <- exps.indices by 2
      } yield {
        if (i + 1 < exps.length) {
          And(exps(i), exps(i + 1))
        } else {
          exps(i)
        }
      }
    }
    exps.length match {
      case 0 =>
        None
      case 1 =>
        Option(exps(0))
      case _ =>
        conjunct(overIndexes().toArray)
    }
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
