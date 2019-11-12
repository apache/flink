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

package org.apache.flink.table.planner.calcite

import org.apache.flink.sql.parser.ExtendedSqlNode
import org.apache.flink.table.api.{TableException, ValidationException}

import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelFieldCollation, RelRoot}
import org.apache.calcite.sql.advise.{SqlAdvisor, SqlAdvisorValidator}
import org.apache.calcite.sql.{SqlKind, SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder, RelConversionException}

import java.lang.{Boolean => JBoolean}
import java.util
import java.util.function.{Function => JFunction}

import scala.collection.JavaConversions._

/**
  * NOTE: this is heavily inspired by Calcite's PlannerImpl.
  * We need it in order to share the planner between the Table API relational plans
  * and the SQL relation plans that are created by the Calcite parser.
  * The main difference is that we do not create a new RelOptPlanner in the ready() method.
  */
class FlinkPlannerImpl(
    config: FrameworkConfig,
    catalogReaderSupplier: JFunction[JBoolean, CalciteCatalogReader],
    typeFactory: FlinkTypeFactory,
    cluster: RelOptCluster) extends FlinkToRelContext {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val parser: CalciteParser = new CalciteParser(config.getParserConfig)
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config = config.getSqlToRelConverterConfig

  var validator: FlinkCalciteSqlValidator = _
  var root: RelRoot = _

  def getCompletionHints(sql: String, cursor: Int): Array[String] = {
    val advisorValidator = new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      config.getParserConfig.conformance())
    val advisor = new SqlAdvisor(advisorValidator, config.getParserConfig)
    val replaced = Array[String](null)
    val hints = advisor.getCompletionHints(sql, cursor, replaced)
      .map(item => item.toIdentifier.toString)
    hints.toArray
  }

  /**
    * Get the [[FlinkCalciteSqlValidator]] instance from this planner, create a new instance
    * if current validator has not been initialized, or returns the validator
    * instance directly.
    *
    * <p>The validator instance creation is not thread safe.
    *
    * @return a new validator instance or current existed one
    */
  def getOrCreateSqlValidator(): FlinkCalciteSqlValidator = {
    if (validator == null) {
      val catalogReader = catalogReaderSupplier.apply(false)
      validator = new FlinkCalciteSqlValidator(
        operatorTable,
        catalogReader,
        typeFactory)
      validator.setIdentifierExpansion(true)
      validator.setDefaultNullCollation(FlinkPlannerImpl.defaultNullCollation)
      // Disable implicit type coercion for now.
      validator.setEnableTypeCoercion(false)
    }
    validator
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    val catalogReader = catalogReaderSupplier.apply(false)
    // do pre-validate rewrite.
    sqlNode.accept(new PreValidateReWriter(catalogReader, typeFactory))
    // do extended validation.
    sqlNode match {
      case node: ExtendedSqlNode =>
        node.validate()
      case _ =>
    }
    // no need to validate row type for DDL and insert nodes.
    if (sqlNode.getKind.belongsTo(SqlKind.DDL)
      || sqlNode.getKind == SqlKind.INSERT) {
      return sqlNode
    }
    try {
      getOrCreateSqlValidator().validate(sqlNode)
    } catch {
      case e: RuntimeException =>
        throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
    }
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    try {
      assert(validatedSqlNode != null)
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        this,
        getOrCreateSqlValidator(),
        catalogReaderSupplier.apply(false),
        cluster,
        convertletTable,
        sqlToRelConverterConfig)
      root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
      // we disable automatic flattening in order to let composite types pass without modification
      // we might enable it again once Calcite has better support for structured types
      // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

      // TableEnvironment.optimize will execute the following
      // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      // convert time indicators
      // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
      root
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  /**
    * Creates a new instance of [[SqlExprToRexConverter]] to convert SQL expression
    * to RexNode.
    */
  def createSqlExprToRexConverter(tableRowType: RelDataType): SqlExprToRexConverter = {
    new SqlExprToRexConverterImpl(config, typeFactory, cluster, tableRowType)
  }

  override def getCluster: RelOptCluster = cluster

  override def expandView(
      rowType: RelDataType,
      queryString: String,
      schemaPath: util.List[String],
      viewPath: util.List[String]): RelRoot = {

    val sqlNode = parser.parse(queryString)
    val catalogReader = catalogReaderSupplier.apply(false)
      .withSchemaPath(schemaPath)
    val validator =
      new FlinkCalciteSqlValidator(operatorTable, catalogReader, typeFactory)
    validator.setIdentifierExpansion(true)
    val validatedSqlNode = validator.validate(sqlNode)
    val sqlToRelConverter = new SqlToRelConverter(
      this,
      validator,
      catalogReader,
      cluster,
      convertletTable,
      sqlToRelConverterConfig)
    var root: RelRoot = sqlToRelConverter.convertQuery(validatedSqlNode, true, false)
    root = root.withRel(sqlToRelConverter.flattenTypes(root.project(), true))
    root.withRel(RelDecorrelator.decorrelateQuery(root.project()))
  }

  override def createRelBuilder(): RelBuilder = {
    sqlToRelConverterConfig.getRelBuilderFactory.create(cluster, null)
  }
}

object FlinkPlannerImpl {

  /**
    * the null default direction if not specified. Consistent with HIVE/SPARK/MYSQL/FLINK-RUNTIME.
    * So the default value only is set [[NullCollation.LOW]] for keeping consistent with
    * FLINK-RUNTIME.
    * [[NullCollation.LOW]] means null values appear first when the order is ASC (ascending), and
    * ordered last when the order is DESC (descending).
    */
  val defaultNullCollation: NullCollation = NullCollation.LOW

  /**
    * the default field collation if not specified, Consistent with CALCITE.
    */
  val defaultCollationDirection: RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
}
