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

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.NullCollation
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptTable.ViewExpander
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.{RelFieldCollation, RelRoot}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.{SqlParser, SqlParseException => CSqlParseException}
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.{SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}
import org.apache.flink.table.api.{SqlParserException, TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.errorcode.CalciteErrorClassifier

import scala.collection.JavaConversions._

/**
  * NOTE: this is heavily inspired by Calcite's PlannerImpl.
  * We need it in order to share the planner between the Table API relational plans
  * and the SQL relation plans that are created by the Calcite parser.
  * The main difference is that we do not create a new RelOptPlanner in the ready() method.
  */
class FlinkPlannerImpl(
    config: FrameworkConfig,
    planner: RelOptPlanner,
    typeFactory: FlinkTypeFactory,
    sqlToRelConverterConfig: SqlToRelConverter.Config,
    cluster: RelOptCluster,
    catalogManager: CatalogManager) {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  /** Holds the trait definitions to be registered with planner. May be null. */
  val traitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = config.getTraitDefs
  val parserConfig: SqlParser.Config = config.getParserConfig
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val defaultSchema: SchemaPlus = config.getDefaultSchema

  var validator: FlinkCalciteSqlValidator = _
  var root: RelRoot = _

  // reuse cluster
  val rexBuilder: RexBuilder = createRexBuilder

  private def ready() {
    if (this.traitDefs != null) {
      planner.clearRelTraitDefs()
      for (traitDef <- this.traitDefs) {
        planner.addRelTraitDef(traitDef)
      }
    }
  }

  def parse(sql: String): SqlNode = {
    try {
      ready()
      val parser: SqlParser = SqlParser.create(sql, parserConfig)
      val sqlNode: SqlNode = parser.parseStmt
      sqlNode
    } catch {
      case e: CSqlParseException =>
        throw SqlParserException(CalciteErrorClassifier.classify(
          "SQL parse failed:\n" + e.getMessage), e)
    }
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    validator = new FlinkCalciteSqlValidator(operatorTable, createCatalogReader, typeFactory)
    validator.setIdentifierExpansion(true)
    validator.setDefaultNullCollation(FlinkPlannerImpl.defaultNullCollation)

    try {
      validator.validate(sqlNode)
    }
    catch {
      case e: RuntimeException =>
        throw new ValidationException(CalciteErrorClassifier.classify(
          "SQL validation failed:\n" + e.getMessage), e)
    }
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    try {
      assert(validatedSqlNode != null)
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        new ViewExpanderImpl, validator, createCatalogReader, cluster, convertletTable,
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

  /** Implements [[org.apache.calcite.plan.RelOptTable.ViewExpander]]
    * interface for [[org.apache.calcite.tools.Planner]]. */
  class ViewExpanderImpl extends ViewExpander {

    override def expandView(
        rowType: RelDataType,
        queryString: String,
        schemaPath: util.List[String],
        viewPath: util.List[String]): RelRoot = {

      val parser: SqlParser = SqlParser.create(queryString, parserConfig)
      var sqlNode: SqlNode = null
      try {
        sqlNode = parser.parseQuery
      }
      catch {
        case e: CSqlParseException =>
          throw SqlParserException(s"SQL parse failed. ${e.getMessage}", e)
      }
      // TODO: To resolve view name automatically, may need to add database as schemaPath too
      val catalogReader: CalciteCatalogReader = createCatalogReader.withSchemaPath(schemaPath)
      val validator: SqlValidator =
        new FlinkCalciteSqlValidator(operatorTable, catalogReader, typeFactory)
      validator.setIdentifierExpansion(true)
      val validatedSqlNode: SqlNode = validator.validate(sqlNode)
      val rexBuilder: RexBuilder = createRexBuilder
      val config: SqlToRelConverter.Config = SqlToRelConverter.configBuilder
        .withTrimUnusedFields(false).withConvertTableAccess(false).build
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        new ViewExpanderImpl, validator, catalogReader, cluster, convertletTable, config)
      root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false)
      root = root.withRel(sqlToRelConverter.flattenTypes(root.project(), true))
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.project()))
      FlinkPlannerImpl.this.root
    }
  }

  private def createCatalogReader: CalciteCatalogReader = {
    val rootSchema: SchemaPlus = FlinkPlannerImpl.rootSchema(defaultSchema)

    new FlinkCalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      catalogManager.getCalciteReaderDefaultPaths(defaultSchema),
      typeFactory,
      CalciteConfig.connectionConfig(parserConfig)
    )
  }

  private def createRexBuilder: RexBuilder = {
    new RexBuilder(typeFactory)
  }

}

object FlinkPlannerImpl {
  private def rootSchema(schema: SchemaPlus): SchemaPlus = {
    if (schema.getParentSchema == null) {
      schema
    }
    else {
      rootSchema(schema.getParentSchema)
    }
  }

  /**
   * the null default direction if not specified. Consistent with HIVE/SPARK/MYSQL/BLINK-RUNTIME.
   * So the default value only is set [[NullCollation.LOW]] for keeping consistent with
   * BLINK-RUNTIME.
   * [[NullCollation.LOW]] means null values appear first when the order is ASC (ascending), and
   * ordered last when the order is DESC (descending).
   */
  val defaultNullCollation: NullCollation = NullCollation.LOW

  /**
    * the default field collation if not specified, Consistent with CALCITE.
    */
  val defaultCollationDirection: RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
}
