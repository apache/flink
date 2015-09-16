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
package org.apache.flink.api.table

import org.apache.flink.api.table.expressions.analysis.{GroupByAnalyzer, PredicateAnalyzer, SelectionAnalyzer}
import org.apache.flink.api.table.expressions.{Expression, ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.plan._

/**
 * The abstraction for writing Table API programs. Similar to how the batch and streaming APIs
 * have [[org.apache.flink.api.scala.DataSet]] and
 * [[org.apache.flink.streaming.api.scala.DataStream]].
 *
 * Use the methods of [[Table]] to transform data. Use
 * [[org.apache.flink.api.java.table.TableEnvironment]] to convert a [[Table]] back to a DataSet
 * or DataStream.
 *
 * When using Scala a [[Table]] can also be converted using implicit conversions.
 *
 * Example:
 *
 * {{{
 *   val table = set.toTable('a, 'b)
 *   ...
 *   val table2 = ...
 *   val set = table2.toDataSet[MyType]
 * }}}
 *
 * Operations such as [[join]], [[select]], [[where]] and [[groupBy]] either take arguments
 * in a Scala DSL or as an expression String. Please refer to the documentation for the expression
 * syntax.
 */
case class Table(private[flink] val operation: PlanNode) {

  /**
   * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
   * can contain complex expressions and aggregations.
   *
   * Example:
   *
   * {{{
   *   in.select('key, 'value.avg + " The average" as 'average, 'other.substring(0, 10))
   * }}}
   */
  def select(fields: Expression*): Table = {
    val analyzer = new SelectionAnalyzer(operation.outputFields)
    val analyzedFields = fields.map(analyzer.analyze)
    val fieldNames = analyzedFields map(_.name)
    if (fieldNames.toSet.size != fieldNames.size) {
      throw new ExpressionException(s"Resulting fields names are not unique in expression" +
        s""" "${fields.mkString(", ")}".""")
    }
    this.copy(operation = Select(operation, analyzedFields))
  }

  /**
   * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
   * can contain complex expressions and aggregations.
   *
   * Example:
   *
   * {{{
   *   in.select("key, value.avg + " The average" as average, other.substring(0, 10)")
   * }}}
   */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

  /**
   * Renames the fields of the expression result. Use this to disambiguate fields before
   * joining to operations.
   *
   * Example:
   *
   * {{{
   *   in.as('a, 'b)
   * }}}
   */
  def as(fields: Expression*): Table = {
    fields forall {
      f => f.isInstanceOf[UnresolvedFieldReference]
    } match {
      case true =>
      case false => throw new ExpressionException("Only field expression allowed in as().")
    }
    this.copy(operation = As(operation, fields.toArray map { _.name }))
  }

  /**
   * Renames the fields of the expression result. Use this to disambiguate fields before
   * joining to operations.
   *
   * Example:
   *
   * {{{
   *   in.as("a, b")
   * }}}
   */
  def as(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    as(fieldExprs: _*)
  }

  /**
   * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
   * clause.
   *
   * Example:
   *
   * {{{
   *   in.filter('name === "Fred")
   * }}}
   */
  def filter(predicate: Expression): Table = {
    val analyzer = new PredicateAnalyzer(operation.outputFields)
    val analyzedPredicate = analyzer.analyze(predicate)
    this.copy(operation = Filter(operation, analyzedPredicate))
  }

  /**
   * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
   * clause.
   *
   * Example:
   *
   * {{{
   *   in.filter("name = 'Fred'")
   * }}}
   */
  def filter(predicate: String): Table = {
    val predicateExpr = ExpressionParser.parseExpression(predicate)
    filter(predicateExpr)
  }

  /**
   * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
   * clause.
   *
   * Example:
   *
   * {{{
   *   in.where('name === "Fred")
   * }}}
   */
  def where(predicate: Expression): Table = {
    filter(predicate)
  }

  /**
   * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
   * clause.
   *
   * Example:
   *
   * {{{
   *   in.where("name = 'Fred'")
   * }}}
   */
  def where(predicate: String): Table = {
    filter(predicate)
  }

  /**
   * Groups the elements on some grouping keys. Use this before a selection with aggregations
   * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
   *
   * Example:
   *
   * {{{
   *   in.groupBy('key).select('key, 'value.avg)
   * }}}
   */
  def groupBy(fields: Expression*): Table = {
    val analyzer = new GroupByAnalyzer(operation.outputFields)
    val analyzedFields = fields.map(analyzer.analyze)

    val illegalKeys = analyzedFields filter {
      case fe: ResolvedFieldReference => false // OK
      case e => true
    }

    if (illegalKeys.nonEmpty) {
      throw new ExpressionException("Illegal key expressions: " + illegalKeys.mkString(", "))
    }

    this.copy(operation = GroupBy(operation, analyzedFields))
  }

  /**
   * Groups the elements on some grouping keys. Use this before a selection with aggregations
   * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
   *
   * Example:
   *
   * {{{
   *   in.groupBy("key").select("key, value.avg")
   * }}}
   */
  def groupBy(fields: String): Table = {
    val fieldsExpr = ExpressionParser.parseExpressionList(fields)
    groupBy(fieldsExpr: _*)
  }

  /**
   * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
   * operations must not overlap, use [[as]] to rename fields if necessary. You can use
   * where and select clauses after a join to further specify the behaviour of the join.
   *
   * Example:
   *
   * {{{
   *   left.join(right).where('a === 'b && 'c > 3).select('a, 'b, 'd)
   * }}}
   */
  def join(right: Table): Table = {
    val leftInputNames = operation.outputFields.map(_._1).toSet
    val rightInputNames = right.operation.outputFields.map(_._1).toSet
    if (leftInputNames.intersect(rightInputNames).nonEmpty) {
      throw new ExpressionException(
        "Overlapping fields names on join input, result would be ambiguous: " +
          operation.outputFields.mkString(", ") +
          " and " +
          right.operation.outputFields.mkString(", ") )
    }
    this.copy(operation = Join(operation, right.operation))
  }

  override def toString: String = s"Expression($operation)"
}
