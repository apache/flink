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
import org.apache.flink.sql.parser.dml.{SqlBeginStatementSet, SqlEndStatementSet}
import org.apache.flink.sql.parser.dql._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader
import com.google.common.collect.ImmutableList
import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.{RelFieldCollation, RelRoot}
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.advise.{SqlAdvisor, SqlAdvisorValidator}
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.{SqlExplain, SqlKind, SqlNode, SqlOperatorTable}
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}
import org.apache.flink.sql.parser.ddl.{SqlReset, SqlSet, SqlUseModules}
import org.apache.flink.table.planner.parse.CalciteParser

import javax.annotation.Nullable
import java.lang.{Boolean => JBoolean}
import java.util
import java.util.function.{Function => JFunction}
import scala.collection.JavaConverters._

/**
  * NOTE: this is heavily inspired by Calcite's PlannerImpl.
  * We need it in order to share the planner between the Table API relational plans
  * and the SQL relation plans that are created by the Calcite parser.
  * The main difference is that we do not create a new RelOptPlanner in the ready() method.
  */
class FlinkPlannerImpl(
    val config: FrameworkConfig,
    catalogReaderSupplier: JFunction[JBoolean, CalciteCatalogReader],
    typeFactory: FlinkTypeFactory,
    cluster: RelOptCluster) {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val parser: CalciteParser = new CalciteParser(config.getParserConfig)
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config = config.getSqlToRelConverterConfig

  var validator: FlinkCalciteSqlValidator = _

  def getSqlAdvisorValidator(): SqlAdvisorValidator = {
      new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withSqlConformance(config.getParserConfig.conformance()))
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

  private def createSqlValidator(catalogReader: CalciteCatalogReader) = {
    val validator = new FlinkCalciteSqlValidator(
      operatorTable,
      catalogReader,
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withIdentifierExpansion(true)
        .withDefaultNullCollation(FlinkPlannerImpl.defaultNullCollation)
        .withTypeCoercionEnabled(false)) // Disable implicit type coercion for now.
    validator
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    val validator = getOrCreateSqlValidator()
    validate(sqlNode, validator)
  }

  private def validate(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator): SqlNode = {
    try {
      sqlNode.accept(new PreValidateReWriter(
        validator, typeFactory))
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
        || sqlNode.isInstanceOf[SqlLoadModule]
        || sqlNode.isInstanceOf[SqlShowCatalogs]
        || sqlNode.isInstanceOf[SqlShowCurrentCatalog]
        || sqlNode.isInstanceOf[SqlShowDatabases]
        || sqlNode.isInstanceOf[SqlShowCurrentDatabase]
        || sqlNode.isInstanceOf[SqlShowTables]
        || sqlNode.isInstanceOf[SqlShowFunctions]
        || sqlNode.isInstanceOf[SqlShowModules]
        || sqlNode.isInstanceOf[SqlShowViews]
        || sqlNode.isInstanceOf[SqlShowPartitions]
        || sqlNode.isInstanceOf[SqlRichDescribeTable]
        || sqlNode.isInstanceOf[SqlUnloadModule]
        || sqlNode.isInstanceOf[SqlUseModules]
        || sqlNode.isInstanceOf[SqlBeginStatementSet]
        || sqlNode.isInstanceOf[SqlEndStatementSet]
        || sqlNode.isInstanceOf[SqlSet]
        || sqlNode.isInstanceOf[SqlReset]) {
        return sqlNode
      }
      sqlNode match {
        case richExplain: SqlRichExplain =>
          val validated = validator.validate(richExplain.getStatement)
          richExplain.setOperand(0, validated)
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
      val sqlToRelConverter: SqlToRelConverter = createSqlToRelConverter(sqlValidator)

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

  def validateExpression(
      sqlNode: SqlNode,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType): SqlNode = {
    validateExpression(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType)
  }

  private def validateExpression(
      sqlNode: SqlNode,
      sqlValidator: FlinkCalciteSqlValidator,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType)
    : SqlNode = {
      val nameToTypeMap = new util.HashMap[String, RelDataType]()
      inputRowType.getFieldList
        .asScala
        .foreach(f => nameToTypeMap.put(f.getName, f.getType))
      if (outputType != null) {
        sqlValidator.setExpectedOutputType(sqlNode, outputType)
      }
      sqlValidator.validateParameterizedExpression(sqlNode, nameToTypeMap)
  }

  def rex(
      sqlNode: SqlNode,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType): RexNode = {
    rex(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType)
  }

  private def rex(
      sqlNode: SqlNode,
      sqlValidator: FlinkCalciteSqlValidator,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType) = {
    try {
      val validatedSqlNode = validateExpression(sqlNode, sqlValidator, inputRowType, outputType)
      val sqlToRelConverter = createSqlToRelConverter(sqlValidator)
      val nameToNodeMap = inputRowType
        .getFieldList
        .asScala
        .map { field =>
          (field.getName, RexInputRef.of(field.getIndex, inputRowType))
        }
        .toMap[String, RexNode]
        .asJava
      sqlToRelConverter.convertExpression(validatedSqlNode, nameToNodeMap)
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private def createSqlToRelConverter(sqlValidator: SqlValidator): SqlToRelConverter = {
    new SqlToRelConverter(
        createToRelContext(),
        sqlValidator,
        sqlValidator.getCatalogReader.unwrap(classOf[CalciteCatalogReader]),
        cluster,
        convertletTable,
        sqlToRelConverterConfig)
  }

  /**
    * Creates a new instance of [[RelOptTable.ToRelContext]] for [[RelOptTable]].
    */
  def createToRelContext(): RelOptTable.ToRelContext = new ToRelContextImpl

  /**
    * Implements [[RelOptTable.ToRelContext]] interface for [[RelOptTable]] and
    * [[org.apache.calcite.tools.Planner]].
    */
  class ToRelContextImpl extends RelOptTable.ToRelContext {

    override def expandView(
        rowType: RelDataType,
        queryString: String,
        schemaPath: util.List[String],
        viewPath: util.List[String])
    : RelRoot = {
      val parsed = parser.parse(queryString)
      val originalReader = catalogReaderSupplier.apply(false)
      val readerWithPathAdjusted = new FlinkCalciteCatalogReader(
        originalReader.getRootSchema,
        List(schemaPath, schemaPath.subList(0, 1)).asJava,
        originalReader.getTypeFactory,
        originalReader.getConfig
      )
      val validator = createSqlValidator(readerWithPathAdjusted)
      val validated = validate(parsed, validator)
      rel(validated, validator)
    }

    override def getCluster: RelOptCluster = cluster

    override def getTableHints: util.List[RelHint] = ImmutableList.of()
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
