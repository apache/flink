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

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptTable.ViewExpander
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.{SqlParser, SqlParseException}
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.{SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter, SqlRexConvertletTable}
import org.apache.calcite.tools.{RelConversionException, ValidationException, Frameworks, FrameworkConfig}
import org.apache.calcite.util.Util
import scala.collection.JavaConversions._

/** NOTE: this is heavily insipred by Calcite's PlannerImpl.
  We need it in order to share the planner between the Table API relational plans
  and the SQL relation plans that are created by the Calcite parser.
  The only difference is that we initialize the RelOptPlanner planner
  when instantiating, instead of creating a new one in the ready() method. **/
class FlinkPlannerImpl(config: FrameworkConfig, var planner: RelOptPlanner) {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  /** Holds the trait definitions to be registered with planner. May be null. */
  val traitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = config.getTraitDefs
  val parserConfig: SqlParser.Config = config.getParserConfig
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  var defaultSchema: SchemaPlus = config.getDefaultSchema

  var typeFactory: JavaTypeFactory = null
  var validator: FlinkCalciteSqlValidator = null
  var validatedSqlNode: SqlNode = null
  var root: RelRoot = null

  private def ready() {
    Frameworks.withPlanner(new Frameworks.PlannerAction[Unit] {
      def apply(
          cluster: RelOptCluster,
          relOptSchema: RelOptSchema,
          rootSchema: SchemaPlus): Unit = {

        Util.discard(rootSchema)
        typeFactory = cluster.getTypeFactory.asInstanceOf[JavaTypeFactory]
        if (planner == null) {
          planner = cluster.getPlanner
        }
      }
    }, config)
    if (this.traitDefs != null) {
      planner.clearRelTraitDefs()
      for (traitDef <- this.traitDefs) {
        planner.addRelTraitDef(traitDef)
      }
    }
  }

  @throws(classOf[SqlParseException])
  def parse(sql: String): SqlNode = {
    ready()
    val parser: SqlParser = SqlParser.create(sql, parserConfig)
    val sqlNode: SqlNode = parser.parseStmt
    sqlNode
  }

  @throws(classOf[ValidationException])
  def validate(sqlNode: SqlNode): SqlNode = {
    validator = new FlinkCalciteSqlValidator(operatorTable, createCatalogReader, typeFactory)
    validator.setIdentifierExpansion(true)
    try {
      validatedSqlNode = validator.validate(sqlNode)
    }
    catch {
      case e: RuntimeException => {
        throw new ValidationException(e)
      }
    }
    validatedSqlNode
  }

  @throws(classOf[RelConversionException])
  def rel(sql: SqlNode): RelRoot = {
    assert(validatedSqlNode != null)
    val rexBuilder: RexBuilder = createRexBuilder
    val cluster: RelOptCluster = RelOptCluster.create(planner, rexBuilder)
    val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
      new ViewExpanderImpl, validator, createCatalogReader, cluster, convertletTable)
    sqlToRelConverter.setTrimUnusedFields(false)
    sqlToRelConverter.enableTableAccessConversion(false)
    root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
    root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
    root
  }

  /** Implements [[org.apache.calcite.plan.RelOptTable.ViewExpander]]
    * interface for [[org.apache.calcite.tools.Planner]]. */
  class ViewExpanderImpl extends ViewExpander {

    override def expandView(
        rowType: RelDataType,
        queryString: String,
        schemaPath: util.List[String]): RelRoot = {

      val parser: SqlParser = SqlParser.create(queryString, parserConfig)
      var sqlNode: SqlNode = null
      try {
        sqlNode = parser.parseQuery
      }
      catch {
        case e: SqlParseException =>
          throw new RuntimeException("parse failed", e)
      }
      val catalogReader: CalciteCatalogReader = createCatalogReader.withSchemaPath(schemaPath)
      val validator: SqlValidator =
        new FlinkCalciteSqlValidator(operatorTable, catalogReader, typeFactory)
      validator.setIdentifierExpansion(true)
      val validatedSqlNode: SqlNode = validator.validate(sqlNode)
      val rexBuilder: RexBuilder = createRexBuilder
      val cluster: RelOptCluster = RelOptCluster.create(planner, rexBuilder)
      val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
        new ViewExpanderImpl, validator, catalogReader, cluster, convertletTable)
      sqlToRelConverter.setTrimUnusedFields(false)
      sqlToRelConverter.enableTableAccessConversion(false)
      root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false)
      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      FlinkPlannerImpl.this.root
    }
  }

  private def createCatalogReader: CalciteCatalogReader = {
    val rootSchema: SchemaPlus = FlinkPlannerImpl.rootSchema(defaultSchema)
    new CalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      parserConfig.caseSensitive,
      CalciteSchema.from(defaultSchema).path(null),
      typeFactory)
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
}
