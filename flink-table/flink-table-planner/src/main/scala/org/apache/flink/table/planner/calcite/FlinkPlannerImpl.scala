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
import org.apache.flink.sql.parser.ddl.{SqlCompilePlan, SqlReset, SqlSet, SqlUseModules}
import org.apache.flink.sql.parser.dml.{RichSqlInsert, SqlBeginStatementSet, SqlCompileAndExecutePlan, SqlEndStatementSet, SqlExecute, SqlExecutePlan, SqlStatementSet, SqlTruncateTable}
import org.apache.flink.sql.parser.dql._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.parse.CalciteParser
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelFieldCollation, RelRoot}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.{SqlBasicCall, SqlCall, SqlHint, SqlKind, SqlNode, SqlNodeList, SqlOperatorTable, SqlProcedureCallOperator, SqlSelect, SqlTableRef}
import org.apache.calcite.sql.advise.SqlAdvisorValidator
import org.apache.calcite.sql.util.SqlShuttle
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}

import javax.annotation.Nullable

import java.lang.{Boolean => JBoolean}
import java.util
import java.util.Locale
import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl. We need it in order to share the planner
 * between the Table API relational plans and the SQL relation plans that are created by the Calcite
 * parser. The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
class FlinkPlannerImpl(
    val config: FrameworkConfig,
    catalogReaderSupplier: JFunction[JBoolean, CalciteCatalogReader],
    typeFactory: FlinkTypeFactory,
    val cluster: RelOptCluster) {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val parser: CalciteParser = new CalciteParser(config.getParserConfig)
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config =
    config.getSqlToRelConverterConfig.withAddJsonTypeOperatorEnabled(false)

  var validator: FlinkCalciteSqlValidator = _

  def getSqlAdvisorValidator(): SqlAdvisorValidator = {
    new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withConformance(config.getParserConfig.conformance()))
  }

  /**
   * Get the [[FlinkCalciteSqlValidator]] instance from this planner, create a new instance if
   * current validator has not been initialized, or returns the validator instance directly.
   *
   * <p>The validator instance creation is not thread safe.
   *
   * @return
   *   a new validator instance or current existed one
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
        .withTypeCoercionEnabled(false),
      createToRelContext(),
      cluster,
      config
    ) // Disable implicit type coercion for now.
    validator
  }

  def validate(sqlNode: SqlNode): SqlNode = {
    val validator = getOrCreateSqlValidator()
    validate(sqlNode, validator)
  }

  private def validate(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator): SqlNode = {
    try {
      sqlNode.accept(new PreValidateReWriter(validator, typeFactory))
      // do extended validation.
      sqlNode match {
        case node: ExtendedSqlNode =>
          node.validate()
        case _ =>
      }
      // no need to validate row type for DDL and insert nodes.
      if (
        sqlNode.getKind.belongsTo(SqlKind.DDL)
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
        || sqlNode.isInstanceOf[SqlShowJars]
        || sqlNode.isInstanceOf[SqlShowModules]
        || sqlNode.isInstanceOf[SqlShowViews]
        || sqlNode.isInstanceOf[SqlShowColumns]
        || sqlNode.isInstanceOf[SqlShowPartitions]
        || sqlNode.isInstanceOf[SqlShowProcedures]
        || sqlNode.isInstanceOf[SqlShowJobs]
        || sqlNode.isInstanceOf[SqlRichDescribeTable]
        || sqlNode.isInstanceOf[SqlUnloadModule]
        || sqlNode.isInstanceOf[SqlUseModules]
        || sqlNode.isInstanceOf[SqlBeginStatementSet]
        || sqlNode.isInstanceOf[SqlEndStatementSet]
        || sqlNode.isInstanceOf[SqlSet]
        || sqlNode.isInstanceOf[SqlReset]
        || sqlNode.isInstanceOf[SqlExecutePlan]
        || sqlNode.isInstanceOf[SqlTruncateTable]
      ) {
        return sqlNode
      }
      sqlNode match {
        case richExplain: SqlRichExplain =>
          val validatedStatement = richExplain.getStatement match {
            // only validate source here
            case insert: RichSqlInsert =>
              validateRichSqlInsert(insert)
            case others =>
              validate(others)
          }
          richExplain.setOperand(0, validatedStatement)
          richExplain
        case statementSet: SqlStatementSet =>
          statementSet.getInserts.asScala.zipWithIndex.foreach {
            case (insert, idx) => statementSet.setOperand(idx, validate(insert))
          }
          statementSet
        case execute: SqlExecute =>
          execute.setOperand(0, validate(execute.getStatement))
          execute
        case insert: RichSqlInsert =>
          validateRichSqlInsert(insert)
        case compile: SqlCompilePlan =>
          compile.setOperand(0, validate(compile.getOperandList.get(0)))
          compile
        case compileAndExecute: SqlCompileAndExecutePlan =>
          compileAndExecute.setOperand(0, validate(compileAndExecute.getOperandList.get(0)))
          compileAndExecute
        // for call procedure statement
        case sqlCallNode if sqlCallNode.getKind == SqlKind.PROCEDURE_CALL =>
          val callNode = sqlCallNode.asInstanceOf[SqlBasicCall]
          callNode.getOperandList.asScala.zipWithIndex.foreach {
            case (operand, idx) => callNode.setOperand(idx, validate(operand))
          }
          callNode
        case _ =>
          validator.validate(sqlNode)
      }
    } catch {
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
      // check whether this SqlNode tree contains join hints
      val checkContainJoinHintShuttle = new CheckContainJoinHintShuttle
      validatedSqlNode.accept(checkContainJoinHintShuttle)
      val sqlToRelConverter: SqlToRelConverter = if (checkContainJoinHintShuttle.containsJoinHint) {
        val converter = createSqlToRelConverter(
          sqlValidator,
          // disable project merge during sql to rel phase to prevent
          // incorrect propagation of join hints into child query block
          sqlToRelConverterConfig.addRelBuilderConfigTransform(c => c.withBloat(-1))
        )
        // TODO currently, it is a relatively hacked way to tell converter
        // that this SqlNode tree contains join hints
        converter.containsJoinHint()
        converter
      } else {
        createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig)
      }

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

  class CheckContainJoinHintShuttle extends SqlShuttle {
    var containsJoinHint: Boolean = false

    override def visit(call: SqlCall): SqlNode = {
      call match {
        case select: SqlSelect =>
          if (select.hasHints && hasJoinHint(select.getHints.getList)) {
            containsJoinHint = true
            return call
          }
        case table: SqlTableRef =>
          val hintList = table.getOperandList.get(1).asInstanceOf[SqlNodeList]
          if (hasJoinHint(hintList.getList)) {
            containsJoinHint = true
            return call
          }
        case _ => // ignore
      }
      super.visit(call)
    }

    private def hasJoinHint(hints: util.List[SqlNode]): Boolean = {
      JavaScalaConversionUtil.toScala(hints).foreach {
        case hint: SqlHint =>
          val hintName = hint.getName
          if (JoinStrategy.isJoinStrategy(hintName.toUpperCase(Locale.ROOT))) {
            return true
          }
      }
      false
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
      @Nullable outputType: RelDataType): SqlNode = {
    val nameToTypeMap = new util.HashMap[String, RelDataType]()
    inputRowType.getFieldList.asScala
      .foreach(f => nameToTypeMap.put(f.getName, f.getType))
    if (outputType != null) {
      sqlValidator.setExpectedOutputType(sqlNode, outputType)
    }
    sqlValidator.validateParameterizedExpression(sqlNode, nameToTypeMap)
  }

  private def validateRichSqlInsert(insert: RichSqlInsert): SqlNode = {
    // We don't support UPSERT INTO semantics (see FLINK-24225).
    if (insert.isUpsert) {
      throw new ValidationException(
        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.")
    }
    // only validate source here.
    // ignore row type which will be verified in table environment.
    val validatedSource = validate(insert.getSource)
    insert.setOperand(2, validatedSource)
    insert
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
      val sqlToRelConverter = createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig)
      val nameToNodeMap = inputRowType.getFieldList.asScala
        .map(field => (field.getName, RexInputRef.of(field.getIndex, inputRowType)))
        .toMap[String, RexNode]
        .asJava
      sqlToRelConverter.convertExpression(validatedSqlNode, nameToNodeMap)
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private def createSqlToRelConverter(
      sqlValidator: SqlValidator,
      config: SqlToRelConverter.Config): SqlToRelConverter = {
    new SqlToRelConverter(
      createToRelContext(),
      sqlValidator,
      sqlValidator.getCatalogReader.unwrap(classOf[CalciteCatalogReader]),
      cluster,
      convertletTable,
      config)
  }

  /** Creates a new instance of [[RelOptTable.ToRelContext]] for [[RelOptTable]]. */
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
        viewPath: util.List[String]): RelRoot = {
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
   * the null default direction if not specified. Consistent with HIVE/SPARK/MYSQL/FLINK-RUNTIME. So
   * the default value only is set [[NullCollation.LOW]] for keeping consistent with FLINK-RUNTIME.
   * [[NullCollation.LOW]] means null values appear first when the order is ASC (ascending), and
   * ordered last when the order is DESC (descending).
   */
  val defaultNullCollation: NullCollation = NullCollation.LOW

  /** the default field collation if not specified, Consistent with CALCITE. */
  val defaultCollationDirection: RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
}
