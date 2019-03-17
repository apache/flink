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
package org.apache.flink.table.api

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions.{Alias, Asc, Expression, ExpressionBridge, ExpressionParser, Ordering, PlannerExpression, ResolvedFieldReference, UnresolvedAlias, WindowProperty}
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.ProjectionTranslator._
import org.apache.flink.table.plan.logical.{Minus, _}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.util.JavaScalaConversionUtil

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.JavaConversions._

/**
  * A Table is the core component of the Table API.
  * Similar to how the batch and streaming APIs have DataSet and DataStream,
  * the Table API is built around [[Table]].
  *
  * Use the methods of [[Table]] to transform data. Use [[TableEnvironment]] to convert a [[Table]]
  * back to a DataSet or DataStream.
  *
  * When using Scala a [[Table]] can also be converted using implicit conversions.
  *
  * Example:
  *
  * {{{
  *   val env = ExecutionEnvironment.getExecutionEnvironment
  *   val tEnv = TableEnvironment.getTableEnvironment(env)
  *
  *   val set: DataSet[(String, Int)] = ...
  *   val table = set.toTable(tEnv, 'a, 'b)
  *   ...
  *   val table2 = ...
  *   val set2: DataSet[MyType] = table2.toDataSet[MyType]
  * }}}
  *
  * Operations such as [[join]], [[select]], [[where]] and [[groupBy]] either take arguments
  * in a Scala DSL or as an expression String. Please refer to the documentation for the expression
  * syntax.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param logicalPlan logical representation
  */
class Table(
    private[flink] val tableEnv: TableEnvironment,
    private[flink] val logicalPlan: LogicalNode) {

  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    tableEnv.expressionBridge

  private lazy val tableSchema: TableSchema = new TableSchema(
    logicalPlan.output.map(_.name).toArray,
    logicalPlan.output.map(_.resultType).toArray)

  var tableName: String = _

  def relBuilder: FlinkRelBuilder = tableEnv.getRelBuilder

  def getRelNode: RelNode = logicalPlan.toRelNode(relBuilder)

  /**
    * Returns the schema of this table.
    */
  def getSchema: TableSchema = tableSchema

  /**
    * Prints the schema of this table to the console in a tree format.
    */
  def printSchema(): Unit = print(tableSchema.toString)

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.select("key, value.avg + ' The average' as average")
    * }}}
    */
  def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.select('key, 'value.avg + " The average" as 'average)
    * }}}
    */
  def select(fields: Expression*): Table = {
    selectInternal(fields.map(expressionBridge.bridge))
  }

  private def selectInternal(fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, logicalPlan, tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, tableEnv)
    if (propNames.nonEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    if (aggNames.nonEmpty) {
      val projectsOnAgg = replaceAggregationsAndProperties(
        expandedFields, tableEnv, aggNames, propNames)
      val projectFields = extractFieldReferences(expandedFields)

      new Table(tableEnv,
        Project(projectsOnAgg,
          Aggregate(Nil, aggNames.map(a => Alias(a._1, a._2)).toSeq,
            Project(projectFields, logicalPlan).validate(tableEnv)
          ).validate(tableEnv)
        ).validate(tableEnv)
      )
    } else {
      new Table(tableEnv,
        Project(expandedFields.map(UnresolvedAlias), logicalPlan).validate(tableEnv))
    }
  }

  /**
    * Creates [[TemporalTableFunction]] backed up by this table as a history table.
    * Temporal Tables represent a concept of a table that changes over time and for which
    * Flink keeps track of those changes. [[TemporalTableFunction]] provides a way how to access
    * those data.
    *
    * For more information please check Flink's documentation on Temporal Tables.
    *
    * Currently [[TemporalTableFunction]]s are only supported in streaming.
    *
    * @param timeAttribute Must points to a time attribute. Provides a way to compare which records
    *                      are a newer or older version.
    * @param primaryKey    Defines the primary key. With primary key it is possible to update
    *                      a row or to delete it.
    * @return [[TemporalTableFunction]] which is an instance of
    *        [[org.apache.flink.table.functions.TableFunction]]. It takes one single argument,
    *        the `timeAttribute`, for which it returns matching version of the [[Table]], from which
    *        [[TemporalTableFunction]] was created.
    */
  def createTemporalTableFunction(
      timeAttribute: String,
      primaryKey: String)
    : TemporalTableFunction = {
    createTemporalTableFunction(
      ExpressionParser.parseExpression(timeAttribute),
      ExpressionParser.parseExpression(primaryKey))
  }

  /**
    * Creates [[TemporalTableFunction]] backed up by this table as a history table.
    * Temporal Tables represent a concept of a table that changes over time and for which
    * Flink keeps track of those changes. [[TemporalTableFunction]] provides a way how to access
    * those data.
    *
    * For more information please check Flink's documentation on Temporal Tables.
    *
    * Currently [[TemporalTableFunction]]s are only supported in streaming.
    *
    * @param timeAttribute Must points to a time indicator. Provides a way to compare which records
    *                      are a newer or older version.
    * @param primaryKey    Defines the primary key. With primary key it is possible to update
    *                      a row or to delete it.
    * @return [[TemporalTableFunction]] which is an instance of
    *        [[org.apache.flink.table.functions.TableFunction]]. It takes one single argument,
    *        the `timeAttribute`, for which it returns matching version of the [[Table]], from which
    *        [[TemporalTableFunction]] was created.
    */
  def createTemporalTableFunction(
      timeAttribute: Expression,
      primaryKey: Expression)
    : TemporalTableFunction = {
    createTemporalTableFunctionInternal(
      expressionBridge.bridge(timeAttribute),
      expressionBridge.bridge(primaryKey))
  }

  private def createTemporalTableFunctionInternal(
      timeAttribute: PlannerExpression,
      primaryKey: PlannerExpression)
    : TemporalTableFunction = {
    val temporalTable = TemporalTable(timeAttribute, primaryKey, logicalPlan)
      .validate(tableEnv)
      .asInstanceOf[TemporalTable]

    TemporalTableFunction.create(
      this,
      temporalTable.timeAttribute,
      validatePrimaryKeyExpression(temporalTable.primaryKey))
  }

  private def validatePrimaryKeyExpression(expression: Expression): String = {
    expression match {
      case fieldReference: ResolvedFieldReference =>
        fieldReference.name
      case _ => throw new ValidationException(
        s"Unsupported expression [$expression] as primary key. " +
          s"Only top-level (not nested) field references are supported.")
    }
  }

  /**
    * Renames the fields of the expression result. Use this to disambiguate fields before
    * joining to operations.
    *
    * Example:
    *
    * {{{
    *   tab.as("a, b")
    * }}}
    */
  def as(fields: String): Table = {
    as(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Renames the fields of the expression result. Use this to disambiguate fields before
    * joining to operations.
    *
    * Example:
    *
    * {{{
    *   tab.as('a, 'b)
    * }}}
    */
  def as(fields: Expression*): Table = {
    asInternal(fields.map(tableEnv.expressionBridge.bridge))
  }

  private def asInternal(fields: Seq[PlannerExpression]): Table = {
    new Table(tableEnv, AliasNode(fields, logicalPlan).validate(tableEnv))
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.filter("name = 'Fred'")
    * }}}
    */
  def filter(predicate: String): Table = {
    filter(ExpressionParser.parseExpression(predicate))
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.filter('name === "Fred")
    * }}}
    */
  def filter(predicate: Expression): Table = {
    filterInternal(expressionBridge.bridge(predicate))
  }

  private def filterInternal(predicate: PlannerExpression): Table = {
    new Table(tableEnv, Filter(predicate, logicalPlan).validate(tableEnv))
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.where("name = 'Fred'")
    * }}}
    */
  def where(predicate: String): Table = {
    filter(predicate)
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.where('name === "Fred")
    * }}}
    */
  def where(predicate: Expression): Table = {
    filter(predicate)
  }

  /**
    * Groups the elements on some grouping keys. Use this before a selection with aggregations
    * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy("key").select("key, value.avg")
    * }}}
    */
  def groupBy(fields: String): GroupedTable = {
    groupBy(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Groups the elements on some grouping keys. Use this before a selection with aggregations
    * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy('key).select('key, 'value.avg)
    * }}}
    */
  def groupBy(fields: Expression*): GroupedTable = {
    groupByInternal(fields.map(expressionBridge.bridge))
  }

  private def groupByInternal(fields: Seq[PlannerExpression]): GroupedTable = {
    new GroupedTable(this, fields)
  }

  /**
    * Removes duplicate values and returns only distinct (different) values.
    *
    * Example:
    *
    * {{{
    *   tab.select("key, value").distinct()
    * }}}
    */
  def distinct(): Table = {
    new Table(tableEnv, Distinct(logicalPlan).validate(tableEnv))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary. You can use
    * where and select clauses after a join to further specify the behaviour of the join.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right).where('a === 'b && 'c > 3).select('a, 'b, 'd)
    * }}}
    */
  def join(right: Table): Table = {
    joinInternal(right, None, JoinType.INNER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right, "a = b")
    * }}}
    */
  def join(right: Table, joinPredicate: String): Table = {
    join(right, ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def join(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.INNER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL left outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.leftOuterJoin(right).select('a, 'b, 'd)
    * }}}
    */
  def leftOuterJoin(right: Table): Table = {
    joinInternal(right, None, JoinType.LEFT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL left outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.leftOuterJoin(right, "a = b").select("a, b, d")
    * }}}
    */
  def leftOuterJoin(right: Table, joinPredicate: String): Table = {
    leftOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL left outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.leftOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def leftOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.LEFT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL right outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.rightOuterJoin(right, "a = b").select('a, 'b, 'd)
    * }}}
    */
  def rightOuterJoin(right: Table, joinPredicate: String): Table = {
    rightOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL right outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.rightOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def rightOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.RIGHT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL full outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.fullOuterJoin(right, "a = b").select('a, 'b, 'd)
    * }}}
    */
  def fullOuterJoin(right: Table, joinPredicate: String): Table = {
    fullOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL full outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have null check enabled (default).
    *
    * Example:
    *
    * {{{
    *   left.fullOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def fullOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.FULL_OUTER)
  }

  private def joinInternal(
      right: Table,
      joinPredicate: Option[PlannerExpression],
      joinType: JoinType)
    : Table = {
    // check that the TableEnvironment of right table is not null
    // and right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be joined.")
    }

    new Table(
      tableEnv,
      Join(this.logicalPlan, right.logicalPlan, joinType, joinPredicate, correlated = false)
        .validate(tableEnv))
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL inner join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table function.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction<String> {
    *     public void eval(String str) {
    *       str.split("#").forEach(this::collect);
    *     }
    *   }
    *
    *   TableFunction<String> split = new MySplitUDTF();
    *   tableEnv.registerFunction("split", split);
    *   table.joinLateral("split(c) as (s)").select("a, b, c, s");
    * }}}
    */
  def joinLateral(tableFunctionCall: String): Table = {
    joinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL inner join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table function.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction[String] {
    *     def eval(str: String): Unit = {
    *       str.split("#").foreach(collect)
    *     }
    *   }
    *
    *   val split = new MySplitUDTF()
    *   table.joinLateral(split('c) as ('s)).select('a, 'b, 'c, 's)
    * }}}
    */
  def joinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(expressionBridge.bridge(tableFunctionCall), None, JoinType.INNER)
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL inner join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table function.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction<String> {
    *     public void eval(String str) {
    *       str.split("#").forEach(this::collect);
    *     }
    *   }
    *
    *   TableFunction<String> split = new MySplitUDTF();
    *   tableEnv.registerFunction("split", split);
    *   table.joinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
    * }}}
    */
  def joinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    joinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL inner join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table function.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction[String] {
    *     def eval(str: String): Unit = {
    *       str.split("#").foreach(collect)
    *     }
    *   }
    *
    *   val split = new MySplitUDTF()
    *   table.joinLateral(split('c) as ('s), 'a === 's).select('a, 'b, 'c, 's)
    * }}}
    */
  def joinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(
      expressionBridge.bridge(tableFunctionCall),
      Some(expressionBridge.bridge(joinPredicate)),
      JoinType.INNER)
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL left outer join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table
    * function. If the table function does not produce any row, the outer row is padded with nulls.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction<String> {
    *     public void eval(String str) {
    *       str.split("#").forEach(this::collect);
    *     }
    *   }
    *
    *   TableFunction<String> split = new MySplitUDTF();
    *   tableEnv.registerFunction("split", split);
    *   table.leftOuterJoinLateral("split(c) as (s)").select("a, b, c, s");
    * }}}
    */
  def leftOuterJoinLateral(tableFunctionCall: String): Table = {
    leftOuterJoinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL left outer join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table
    * function. If the table function does not produce any row, the outer row is padded with nulls.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction[String] {
    *     def eval(str: String): Unit = {
    *       str.split("#").foreach(collect)
    *     }
    *   }
    *
    *   val split = new MySplitUDTF()
    *   table.leftOuterJoinLateral(split('c) as ('s)).select('a, 'b, 'c, 's)
    * }}}
    */
  def leftOuterJoinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(expressionBridge.bridge(tableFunctionCall), None, JoinType.LEFT_OUTER)
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL left outer join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table
    * function. If the table function does not produce any row, the outer row is padded with nulls.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction<String> {
    *     public void eval(String str) {
    *       str.split("#").forEach(this::collect);
    *     }
    *   }
    *
    *   TableFunction<String> split = new MySplitUDTF();
    *   tableEnv.registerFunction("split", split);
    *   table.leftOuterJoinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
    * }}}
    */
  def leftOuterJoinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    leftOuterJoinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  /**
    * Joins this [[Table]] with an user-defined [[org.apache.flink.table.functions.TableFunction]].
    * This join is similar to a SQL left outer join with ON TRUE predicate but works with a
    * table function. Each row of the table is joined with all rows produced by the table
    * function. If the table function does not produce any row, the outer row is padded with nulls.
    *
    * Example:
    * {{{
    *   class MySplitUDTF extends TableFunction[String] {
    *     def eval(str: String): Unit = {
    *       str.split("#").foreach(collect)
    *     }
    *   }
    *
    *   val split = new MySplitUDTF()
    *   table.leftOuterJoinLateral(split('c) as ('s), 'a === 's).select('a, 'b, 'c, 's)
    * }}}
    */
  def leftOuterJoinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(
      expressionBridge.bridge(tableFunctionCall),
      Some(expressionBridge.bridge(joinPredicate)),
      JoinType.LEFT_OUTER)
  }

  private def joinLateralInternal(
      callExpr: PlannerExpression,
      joinPredicate: Option[PlannerExpression],
      joinType: JoinType): Table = {

    // check join type
    if (joinType != JoinType.INNER && joinType != JoinType.LEFT_OUTER) {
      throw new ValidationException(
        "Table functions are currently only supported for inner and left outer lateral joins.")
    }

    val logicalCall = UserDefinedFunctionUtils.createLogicalFunctionCall(
      callExpr,
      logicalPlan)
    val validatedLogicalCall = logicalCall.validate(tableEnv)

    new Table(
      tableEnv,
      Join(
        logicalPlan,
        validatedLogicalCall,
        joinType,
        joinPredicate,
        correlated = true
      ).validate(tableEnv))
  }

  /**
    * Minus of two [[Table]]s with duplicate records removed.
    * Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not
    * exist in the right table. Duplicate records in the left table are returned
    * exactly once, i.e., duplicates are removed. Both tables must have identical field types.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.minus(right)
    * }}}
    */
  def minus(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new Table(tableEnv, Minus(logicalPlan, right.logicalPlan, all = false)
      .validate(tableEnv))
  }

  /**
    * Minus of two [[Table]]s. Similar to an SQL EXCEPT ALL.
    * Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in
    * the right table. A record that is present n times in the left table and m times
    * in the right table is returned (n - m) times, i.e., as many duplicates as are present
    * in the right table are removed. Both tables must have identical field types.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.minusAll(right)
    * }}}
    */
  def minusAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new Table(tableEnv, Minus(logicalPlan, right.logicalPlan, all = true)
      .validate(tableEnv))
  }

  /**
    * Unions two [[Table]]s with duplicate records removed.
    * Similar to an SQL UNION. The fields of the two union operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.union(right)
    * }}}
    */
  def union(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, all = false).validate(tableEnv))
  }

  /**
    * Unions two [[Table]]s. Similar to an SQL UNION ALL. The fields of the two union operations
    * must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.unionAll(right)
    * }}}
    */
  def unionAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, all = true).validate(tableEnv))
  }

  /**
    * Intersects two [[Table]]s with duplicate records removed. Intersect returns records that
    * exist in both tables. If a record is present in one or both tables more than once, it is
    * returned just once, i.e., the resulting table has no duplicate records. Similar to an
    * SQL INTERSECT. The fields of the two intersect operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.intersect(right)
    * }}}
    */
  def intersect(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new Table(tableEnv, Intersect(logicalPlan, right.logicalPlan, all = false).validate(tableEnv))
  }

  /**
    * Intersects two [[Table]]s. IntersectAll returns records that exist in both tables.
    * If a record is present in both tables more than once, it is returned as many times as it
    * is present in both tables, i.e., the resulting table might have duplicate records. Similar
    * to an SQL INTERSECT ALL. The fields of the two intersect operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.intersectAll(right)
    * }}}
    */
  def intersectAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new Table(tableEnv, Intersect(logicalPlan, right.logicalPlan, all = true).validate(tableEnv))
  }

  /**
    * Sorts the given [[Table]]. Similar to SQL ORDER BY.
    * The resulting Table is globally sorted across all parallel partitions.
    *
    * Example:
    *
    * {{{
    *   tab.orderBy("name.desc")
    * }}}
    */
  def orderBy(fields: String): Table = {
    orderBy(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Sorts the given [[Table]]. Similar to SQL ORDER BY.
    * The resulting Table is globally sorted across all parallel partitions.
    *
    * Example:
    *
    * {{{
    *   tab.orderBy('name.desc)
    * }}}
    */
  def orderBy(fields: Expression*): Table = {
    orderByInternal(fields.map(expressionBridge.bridge))
  }

  private def orderByInternal(fields: Seq[PlannerExpression]): Table = {
    val order: Seq[Ordering] = fields.map {
      case o: Ordering => o
      case e => Asc(e)
    }
    new Table(tableEnv, Sort(order, logicalPlan).validate(tableEnv))
  }

  /**
    * Limits a sorted result from an offset position.
    * Similar to a SQL OFFSET clause. Offset is technically part of the Order By operator and
    * thus must be preceded by it.
    *
    * [[Table.offset(o)]] can be combined with a subsequent [[Table.fetch(n)]] call to return n rows
    * after skipping the first o rows.
    *
    * {{{
    *   // skips the first 3 rows and returns all following rows.
    *   tab.orderBy('name.desc).offset(3)
    *   // skips the first 10 rows and returns the next 5 rows.
    *   tab.orderBy('name.desc).offset(10).fetch(5)
    * }}}
    *
    * @param offset number of records to skip
    */
  def offset(offset: Int): Table = {
    new Table(tableEnv, Limit(offset, -1, logicalPlan).validate(tableEnv))
  }

  /**
    * Limits a sorted result to the first n rows.
    * Similar to a SQL FETCH clause. Fetch is technically part of the Order By operator and
    * thus must be preceded by it.
    *
    * [[Table.fetch(n)]] can be combined with a preceding [[Table.offset(o)]] call to return n rows
    * after skipping the first o rows.
    *
    * {{{
    *   // returns the first 3 records.
    *   tab.orderBy('name.desc).fetch(3)
    *   // skips the first 10 rows and returns the next 5 rows.
    *   tab.orderBy('name.desc).offset(10).fetch(5)
    * }}}
    *
    * @param fetch the number of records to return. Fetch must be >= 0.
    */
  def fetch(fetch: Int): Table = {
    if (fetch < 0) {
      throw new ValidationException("FETCH count must be equal or larger than 0.")
    }
    this.logicalPlan match {
      case Limit(o, -1, c) =>
        // replace LIMIT without FETCH by LIMIT with FETCH
        new Table(tableEnv, Limit(o, fetch, c).validate(tableEnv))
      case Limit(_, _, _) =>
        throw new ValidationException("FETCH is already defined.")
      case _ =>
        new Table(tableEnv, Limit(0, fetch, logicalPlan).validate(tableEnv))
    }
  }

  /**
    * Writes the [[Table]] to a [[TableSink]]. A [[TableSink]] defines an external storage location.
    *
    * A batch [[Table]] can only be written to a
    * [[org.apache.flink.table.sinks.BatchTableSink]], a streaming [[Table]] requires a
    * [[org.apache.flink.table.sinks.AppendStreamTableSink]], a
    * [[org.apache.flink.table.sinks.RetractStreamTableSink]], or an
    * [[org.apache.flink.table.sinks.UpsertStreamTableSink]].
    *
    * @param sink The [[TableSink]] to which the [[Table]] is written.
    * @tparam T The data type that the [[TableSink]] expects.
    *
    * @deprecated Will be removed in a future release. Please register the TableSink and use
    *             Table.insertInto().
    */
  @deprecated("This method will be removed. Please register the TableSink and use " +
    "Table.insertInto().", "1.7.0")
  @Deprecated
  def writeToSink[T](sink: TableSink[T]): Unit = {
    val queryConfig = Option(this.tableEnv) match {
      case None => null
      case _ => this.tableEnv.queryConfig
    }
    writeToSink(sink, queryConfig)
  }

  /**
    * Writes the [[Table]] to a [[TableSink]]. A [[TableSink]] defines an external storage location.
    *
    * A batch [[Table]] can only be written to a
    * [[org.apache.flink.table.sinks.BatchTableSink]], a streaming [[Table]] requires a
    * [[org.apache.flink.table.sinks.AppendStreamTableSink]], a
    * [[org.apache.flink.table.sinks.RetractStreamTableSink]], or an
    * [[org.apache.flink.table.sinks.UpsertStreamTableSink]].
    *
    * @param sink The [[TableSink]] to which the [[Table]] is written.
    * @param conf The configuration for the query that writes to the sink.
    * @tparam T The data type that the [[TableSink]] expects.
    *
    * @deprecated Will be removed in a future release. Please register the TableSink and use
    *             Table.insertInto().
    */
  @deprecated("This method will be removed. Please register the TableSink and use " +
    "Table.insertInto().", "1.7.0")
  @Deprecated
  def writeToSink[T](sink: TableSink[T], conf: QueryConfig): Unit = {
    // get schema information of table
    val rowType = getRelNode.getRowType
    val fieldNames: Array[String] = rowType.getFieldNames.asScala.toArray
    val fieldTypes: Array[TypeInformation[_]] = rowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
      .map {
        // replace time indicator types by SQL_TIMESTAMP
        case t: TypeInformation[_] if FlinkTypeFactory.isTimeIndicatorType(t) => Types.SQL_TIMESTAMP
        case t: TypeInformation[_] => t
      }.toArray

    // configure the table sink
    val configuredSink = sink.configure(fieldNames, fieldTypes)

    // emit the table to the configured table sink
    tableEnv.writeToSink(this, configuredSink, conf)
  }

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * A batch [[Table]] can only be written to a
    * [[org.apache.flink.table.sinks.BatchTableSink]], a streaming [[Table]] requires a
    * [[org.apache.flink.table.sinks.AppendStreamTableSink]], a
    * [[org.apache.flink.table.sinks.RetractStreamTableSink]], or an
    * [[org.apache.flink.table.sinks.UpsertStreamTableSink]].
    *
    * @param tableName Name of the registered [[TableSink]] to which the [[Table]] is written.
    */
  def insertInto(tableName: String): Unit = {
    insertInto(tableName, tableEnv.queryConfig)
  }

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * A batch [[Table]] can only be written to a
    * [[org.apache.flink.table.sinks.BatchTableSink]], a streaming [[Table]] requires a
    * [[org.apache.flink.table.sinks.AppendStreamTableSink]], a
    * [[org.apache.flink.table.sinks.RetractStreamTableSink]], or an
    * [[org.apache.flink.table.sinks.UpsertStreamTableSink]].
    *
    * @param tableName Name of the [[TableSink]] to which the [[Table]] is written.
    * @param conf The [[QueryConfig]] to use.
    */
  def insertInto(tableName: String, conf: QueryConfig): Unit = {
    tableEnv.insertInto(this, tableName, conf)
  }

  /**
    * Groups the records of a table by assigning them to windows defined by a time or row interval.
    *
    * For streaming tables of infinite size, grouping into windows is required to define finite
    * groups on which group-based aggregates can be computed.
    *
    * For batch tables of finite size, windowing essentially provides shortcuts for time-based
    * groupBy.
    *
    * __Note__: Computing windowed aggregates on a streaming table is only a parallel operation
    * if additional grouping attributes are added to the `groupBy(...)` clause.
    * If the `groupBy(...)` only references a window alias, the streamed table will be processed
    * by a single task, i.e., with parallelism 1.
    *
    * @param window groupWindow that specifies how elements are grouped.
    * @return A group windowed table.
    */
  def window(window: GroupWindow): GroupWindowedTable = {
    new GroupWindowedTable(this, window)
  }

  /**
    * Defines over-windows on the records of a table.
    *
    * An over-window defines for each record an interval of records over which aggregation
    * functions can be computed.
    *
    * Example:
    *
    * {{{
    *   table
    *     .window(Over partitionBy 'c orderBy 'rowTime preceding 10.seconds as 'ow)
    *     .select('c, 'b.count over 'ow, 'e.sum over 'ow)
    * }}}
    *
    * __Note__: Computing over window aggregates on a streaming table is only a parallel operation
    * if the window is partitioned. Otherwise, the whole stream will be processed by a single
    * task, i.e., with parallelism 1.
    *
    * __Note__: Over-windows for batch tables are currently not supported.
    *
    * @param overWindows windows that specify the record interval over which aggregations are
    *                    computed.
    * @return An OverWindowedTable to specify the aggregations.
    */
  @varargs
  def window(overWindows: OverWindow*): OverWindowedTable = {

    if (tableEnv.isInstanceOf[BatchTableEnvironment]) {
      throw new TableException("Over-windows for batch tables are currently not supported.")
    }

    if (overWindows.size != 1) {
      throw new TableException("Over-Windows are currently only supported single window.")
    }

    new OverWindowedTable(this, overWindows)
  }

  /**
    * Registers an unique table name under the table environment
    * and return the registered table name.
    */
  override def toString: String = {
    if (tableName == null) {
      tableName = "UnnamedTable$" + tableEnv.attrNameCntr.getAndIncrement()
      tableEnv.registerTable(tableName, this)
    }
    tableName
  }
}

/**
  * A table that has been grouped on a set of grouping keys.
  */
class GroupedTable(
  private[flink] val table: Table,
  private[flink] val groupKey: Seq[PlannerExpression]) {

  /**
    * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy("key").select("key, value.avg + ' The average' as average")
    * }}}
    */
  def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy('key).select('key, 'value.avg + " The average" as 'average)
    * }}}
    */
  def select(fields: Expression*): Table = {
    selectInternal(fields.map(table.expressionBridge.bridge))
  }

  private def selectInternal(fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, table.logicalPlan, table.tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, table.tableEnv)
    if (propNames.nonEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    val projectsOnAgg = replaceAggregationsAndProperties(
      expandedFields, table.tableEnv, aggNames, propNames)
    val projectFields = extractFieldReferences(expandedFields ++ groupKey)

    new Table(table.tableEnv,
      Project(projectsOnAgg,
        Aggregate(groupKey, aggNames.map(a => Alias(a._1, a._2)).toSeq,
          Project(projectFields, table.logicalPlan).validate(table.tableEnv)
        ).validate(table.tableEnv)
      ).validate(table.tableEnv))
  }
}

/**
  * A table that has been windowed for [[GroupWindow]]s.
  */
class GroupWindowedTable(
    private[flink] val table: Table,
    private[flink] val window: GroupWindow) {

  /**
    * Groups the elements by a mandatory window and one or more optional grouping attributes.
    * The window is specified by referring to its alias.
    *
    * If no additional grouping attribute is specified and if the input is a streaming table,
    * the aggregation will be performed by a single task, i.e., with parallelism 1.
    *
    * Aggregations are performed per group and defined by a subsequent `select(...)` clause similar
    * to SQL SELECT-GROUP-BY query.
    *
    * Example:
    *
    * {{{
    *   tab.window([window].as("w")).groupBy("w, key").select("key, value.avg")
    * }}}
    */
  def groupBy(fields: String): WindowGroupedTable = {
    groupBy(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Groups the elements by a mandatory window and one or more optional grouping attributes.
    * The window is specified by referring to its alias.
    *
    * If no additional grouping attribute is specified and if the input is a streaming table,
    * the aggregation will be performed by a single task, i.e., with parallelism 1.
    *
    * Aggregations are performed per group and defined by a subsequent `select(...)` clause similar
    * to SQL SELECT-GROUP-BY query.
    *
    * Example:
    *
    * {{{
    *   tab.window([window] as 'w)).groupBy('w, 'key).select('key, 'value.avg)
    * }}}
    */
  def groupBy(fields: Expression*): WindowGroupedTable = {
    val fieldsWithoutWindow = fields.filterNot(window.getAlias.equals(_))
    if (fields.size != fieldsWithoutWindow.size + 1) {
      throw new ValidationException("GroupBy must contain exactly one window alias.")
    }

    new WindowGroupedTable(table, fieldsWithoutWindow, window)
  }
}

/**
  * A table that has been windowed and grouped for [[GroupWindow]]s.
  */
class WindowGroupedTable(
    private[flink] val table: Table,
    private[flink] val groupKeys: Seq[Expression],
    private[flink] val window: GroupWindow) {

  /**
    * Performs a selection operation on a window grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   windowGroupedTable.select("key, window.start, value.avg as valavg")
    * }}}
    */
  def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Performs a selection operation on a window grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   windowGroupedTable.select('key, 'window.start, 'value.avg as 'valavg)
    * }}}
    */
  def select(fields: Expression*): Table = {
    selectInternal(
      groupKeys.map(table.expressionBridge.bridge),
      createLogicalWindow(),
      fields.map(table.expressionBridge.bridge))
  }

  private def selectInternal(
      groupKeys: Seq[PlannerExpression],
      window: LogicalWindow,
      fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, table.logicalPlan, table.tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, table.tableEnv)

    val projectsOnAgg = replaceAggregationsAndProperties(
      expandedFields, table.tableEnv, aggNames, propNames)

    val projectFields = extractFieldReferences(expandedFields ++ groupKeys :+ window.timeAttribute)

    new Table(table.tableEnv,
      Project(
        projectsOnAgg,
        WindowAggregate(
          groupKeys,
          window,
          propNames.map(a => Alias(a._1, a._2)).toSeq,
          aggNames.map(a => Alias(a._1, a._2)).toSeq,
          Project(projectFields, table.logicalPlan).validate(table.tableEnv)
        ).validate(table.tableEnv),
        // required for proper resolution of the time attribute in multi-windows
        explicitAlias = true
      ).validate(table.tableEnv))
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(): LogicalWindow = window match {
    case tw: TumbleWithSizeOnTimeWithAlias =>
      TumblingGroupWindow(
        table.expressionBridge.bridge(tw.getAlias),
        table.expressionBridge.bridge(tw.getTimeField),
        table.expressionBridge.bridge(tw.getSize))
    case sw: SlideWithSizeAndSlideOnTimeWithAlias =>
      SlidingGroupWindow(
        table.expressionBridge.bridge(sw.getAlias),
        table.expressionBridge.bridge(sw.getTimeField),
        table.expressionBridge.bridge(sw.getSize),
        table.expressionBridge.bridge(sw.getSlide))
    case sw: SessionWithGapOnTimeWithAlias =>
      SessionGroupWindow(
        table.expressionBridge.bridge(sw.getAlias),
        table.expressionBridge.bridge(sw.getTimeField),
        table.expressionBridge.bridge(sw.getGap))
  }
}

/**
  * A table that has been windowed for [[OverWindow]]s.
  */
class OverWindowedTable(
    private[flink] val table: Table,
    private[flink] val overWindows: Seq[OverWindow]) {

  /**
    * Performs a selection operation on an over-windowed table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   overWindowedTable.select("c, b.count over ow, e.sum over ow")
    * }}}
    */
  def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  /**
    * Performs a selection operation on an over-windowed table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   overWindowedTable.select('c, 'b.count over 'ow, 'e.sum over 'ow)
    * }}}
    */
  def select(fields: Expression*): Table = {
    selectInternal(
      fields.map(table.expressionBridge.bridge),
      overWindows.map(createLogicalWindow))
  }

  private def selectInternal(
      fields: Seq[PlannerExpression],
      logicalOverWindows: Seq[LogicalOverWindow])
    : Table = {

    val expandedFields = expandProjectList(
      fields,
      table.logicalPlan,
      table.tableEnv)

    if (fields.exists(_.isInstanceOf[WindowProperty])){
      throw new ValidationException(
        "Window start and end properties are not available for Over windows.")
    }

    val expandedOverFields = resolveOverWindows(expandedFields, logicalOverWindows, table.tableEnv)

    new Table(
      table.tableEnv,
      Project(
        expandedOverFields.map(UnresolvedAlias),
        table.logicalPlan,
        // required for proper projection push down
        explicitAlias = true)
        .validate(table.tableEnv)
    )
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(overWindow: OverWindow): LogicalOverWindow = {
    LogicalOverWindow(
      table.expressionBridge.bridge(overWindow.getAlias),
      overWindow.getPartitioning.map(table.expressionBridge.bridge),
      table.expressionBridge.bridge(overWindow.getOrder),
      table.expressionBridge.bridge(overWindow.getPreceding),
      JavaScalaConversionUtil.toScala(overWindow.getFollowing).map(table.expressionBridge.bridge)
    )
  }
}
