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

package org.apache.flink.table.calcite

import org.apache.flink.sql.parser.ExtendedSqlNode
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.sql.parser.dql.{SqlRichDescribeTable, SqlRichExplain, SqlShowCatalogs,
  SqlShowCurrentCatalog, SqlShowCurrentDatabase, SqlShowDatabases,
  SqlShowFunctions, SqlShowTables, SqlShowViews}
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogReader

import org.apache.calcite.plan.RelOptTable.ViewExpander
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.advise.{SqlAdvisor, SqlAdvisorValidator}
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.{SqlExplain, SqlKind, SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.util
import _root_.java.util.function.{Function => JFunction}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl.
 * We need it in order to share the planner between the Table API relational plans
 * and the SQL relation plans that are created by the Calcite parser.
 * The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
class FlinkPlannerImpl(
    val config: FrameworkConfig,
    val catalogReaderSupplier: JFunction[JBoolean, CatalogReader],
    planner: RelOptPlanner,
    val typeFactory: FlinkTypeFactory)
  extends ViewExpander {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val parser: CalciteParser = new CalciteParser(config.getParserConfig)
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config = config.getSqlToRelConverterConfig

  var validator: FlinkCalciteSqlValidator = _

  def getCompletionHints(sql: String, cursor: Int): Array[String] = {
    val advisorValidator = new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      SqlValidator.Config.DEFAULT
          .withSqlConformance(config.getParserConfig.conformance()))
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
      validator = createSqlValidator(catalogReader)
    }
    validator
  }

  private def createSqlValidator(catalogReader: CatalogReader) = {
    val validator = new FlinkCalciteSqlValidator(
      operatorTable,
      catalogReader,
      typeFactory,
      SqlValidator.Config.DEFAULT
          .withIdentifierExpansion(true)
          // Disable implicit type coercion for now.
          .withTypeCoercionEnabled(false))
    validator
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    val validator = getOrCreateSqlValidator()
    validateInternal(sqlNode, validator)
  }

  private def validateInternal(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator): SqlNode = {
    try {
      sqlNode.accept(new PreValidateReWriter(
        validator.getCatalogReader.unwrap(classOf[CatalogReader]), typeFactory))
      // do extended validation.
      sqlNode match {
        case node: ExtendedSqlNode =>
          node.validate()
        case _ =>
      }
      // no need to validate row type for DDL and insert nodes.
      if (sqlNode.getKind.belongsTo(SqlKind.DDL)
        || sqlNode.getKind == SqlKind.INSERT
        || sqlNode.getKind == SqlKind.CREATE_FUNCTION
        || sqlNode.getKind == SqlKind.DROP_FUNCTION
        || sqlNode.getKind == SqlKind.OTHER_DDL
        || sqlNode.isInstanceOf[SqlShowCatalogs]
        || sqlNode.isInstanceOf[SqlShowCurrentCatalog]
        || sqlNode.isInstanceOf[SqlShowDatabases]
        || sqlNode.isInstanceOf[SqlShowCurrentDatabase]
        || sqlNode.isInstanceOf[SqlShowTables]
        || sqlNode.isInstanceOf[SqlShowFunctions]
        || sqlNode.isInstanceOf[SqlShowViews]
        || sqlNode.isInstanceOf[SqlRichDescribeTable]) {
        return sqlNode
      }
      sqlNode match {
        case richExplain: SqlRichExplain =>
          val validatedStatement = richExplain.getStatement match {
            case insert: RichSqlInsert =>
              val validatedSource = validator.validate(insert.getSource)
              insert.setOperand(2, validatedSource)
              insert
            case others =>
              validator.validate(others)
          }
          richExplain.setOperand(0, validatedStatement)
          richExplain
        case _ =>
          validator.validate(sqlNode)
      }
    }
    catch {
      case e: RuntimeException =>
        throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
    }
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    rel(validatedSqlNode, getOrCreateSqlValidator())
  }

  private def rel(validatedSqlNode: SqlNode, sqlValidator: FlinkCalciteSqlValidator) = {
    try {
      assert(validatedSqlNode != null)
      val rexBuilder: RexBuilder = createRexBuilder
      val cluster: RelOptCluster = FlinkRelOptClusterFactory.create(planner, rexBuilder)
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        this,
        sqlValidator,
        sqlValidator.getCatalogReader.unwrap(classOf[CatalogReader]),
        cluster,
        convertletTable,
        sqlToRelConverterConfig)
      sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
      // we disable automatic flattening in order to let composite types pass without modification
      // we might enable it again once Calcite has better support for structured types
      // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

      // TableEnvironment.optimize will execute the following
      // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      // convert time indicators
      // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  override def expandView(
      rowType: RelDataType,
      queryString: String,
      schemaPath: util.List[String],
      viewPath: util.List[String])
    : RelRoot = {
    val parsed = parser.parse(queryString)
    val originalReader = catalogReaderSupplier.apply(false)
    val readerWithPathAdjusted = new CatalogReader(
      originalReader.getRootSchema,
      List(schemaPath, schemaPath.subList(0, 1)).asJava,
      originalReader.getTypeFactory,
      originalReader.getConfig
    )
    val validator = createSqlValidator(readerWithPathAdjusted)
    val validated = validateInternal(parsed, validator)
    rel(validated, validator)
  }

  private def createRexBuilder: RexBuilder = {
    new RexBuilder(typeFactory)
  }
}
