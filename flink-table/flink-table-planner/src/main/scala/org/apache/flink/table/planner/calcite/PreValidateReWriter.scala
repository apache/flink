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

import org.apache.flink.sql.parser.SqlProperty
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.sql.parser.dql.SqlRichExplain
import org.apache.flink.table.planner.calcite.PreValidateReWriter.validateInsertTargets
import org.apache.flink.table.planner.plan.schema.{CatalogSourceTable, FlinkPreparingTableBase, LegacyCatalogSourceTable}

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.runtime.{CalciteContextException, Resources}
import org.apache.calcite.sql.{SqlCall, SqlIdentifier, SqlKind, SqlNode, SqlSelect, SqlTableRef, SqlUtil}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.validate.{SqlValidatorException, SqlValidatorTable, SqlValidatorUtil}
import org.apache.calcite.util.Static.RESOURCE

import java.util

import scala.collection.JavaConversions._

/**
 * Implements [[org.apache.calcite.sql.util.SqlVisitor]] interface to validate the static partitions
 * and the target column list of an INSERT statement before its source query is validated.
 *
 * <p>The reordering of the source columns and the padding of unlisted columns with NULL or with the
 * static partition values is not applied to the SQL AST here. It is applied to the relational plan
 * after validation, see [[org.apache.flink.table.planner.operations.converters.PartialInsertUtil]].
 * Rewriting the source query here required validating it once for the rewrite and once for real,
 * which left the table arguments of set-semantic process table functions unvalidated (the same
 * problem FLINK-40039 fixed for CTAS/RTAS).
 */
class PreValidateReWriter(
    val validator: FlinkCalciteSqlValidator,
    val typeFactory: RelDataTypeFactory)
  extends SqlBasicVisitor[Unit] {
  override def visit(call: SqlCall): Unit = {
    call match {
      case e: SqlRichExplain =>
        e.getStatement match {
          case r: RichSqlInsert => validateInsertTargets(r, validator, typeFactory)
          case _ => // do nothing
        }
      case r: RichSqlInsert => validateInsertTargets(r, validator, typeFactory)
      case _ => // do nothing
    }
  }
}

object PreValidateReWriter {

  /**
   * Validates the static partitions and the target column list of an INSERT statement against the
   * persisted schema of the target table.
   *
   * <p>For a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt;) whose partition columns are
   * (&lt;a&gt;, &lt;c&gt;) and a query <blockquote><pre> insert into A partition(a='11', c='22')
   * select b from B </pre></blockquote> or a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt;)
   * and a query <blockquote><pre> insert into A (a, b) select a, b from B </pre></blockquote> every
   * referenced column must exist in A, no column may be assigned twice, every unlisted column must
   * be nullable because it is padded with NULL later, and the source must produce exactly one
   * column per listed target column where this can be decided without validating the source.
   *
   * @param sqlInsert
   *   RichSqlInsert instance
   * @param validator
   *   Validator
   * @param typeFactory
   *   type factory
   */
  def validateInsertTargets(
      sqlInsert: RichSqlInsert,
      validator: FlinkCalciteSqlValidator,
      typeFactory: RelDataTypeFactory): Unit = {
    val partitions = sqlInsert.getStaticPartitions
    if (partitions.isEmpty && sqlInsert.getTargetColumnList == null) {
      return
    }
    val calciteCatalogReader = validator.getCatalogReader.unwrap(classOf[CalciteCatalogReader])
    val names = sqlInsert.getTargetTable match {
      case si: SqlIdentifier => si.names
      case st: SqlTableRef => st.getOperandList.get(0).asInstanceOf[SqlIdentifier].names
    }
    val table = calciteCatalogReader.getTable(names)
    if (table == null) {
      // There is no table exists in current catalog,
      // just skip to let other validation error throw.
      return
    }

    val targetRowType = createTargetRowType(typeFactory, table)
    val relOptTable = table match {
      case t: RelOptTable => t
      case _ => null
    }
    val assignedFields = new util.HashSet[Integer]

    // validate partition fields first.
    val partitionColumns = partitions.getList.map {
      node =>
        val id = node.asInstanceOf[SqlProperty].getKey
        validateUnsupportedCompositeColumn(id)
        val targetField = SqlValidatorUtil.getTargetField(
          targetRowType,
          typeFactory,
          id,
          calciteCatalogReader,
          relOptTable)
        validateField(assignedFields.add, id, targetField)
        targetField
    }

    if (sqlInsert.getTargetColumnList == null) {
      return
    }

    // validate partial insert columns.
    val targetColumns = sqlInsert.getTargetColumnList.getList.map {
      id =>
        val identifier = id.asInstanceOf[SqlIdentifier]
        validateUnsupportedCompositeColumn(identifier)
        val targetField = SqlValidatorUtil.getTargetField(
          targetRowType,
          typeFactory,
          identifier,
          calciteCatalogReader,
          relOptTable)
        validateField(assignedFields.add, identifier, targetField)
        targetField
    }

    // unlisted columns are padded with NULL after validation, which requires them to be nullable
    for (targetField <- targetRowType.getFieldList) {
      if (
        !partitionColumns.contains(targetField) && !targetColumns.contains(targetField)
        && !targetField.getType.isNullable
      ) {
        val id = new SqlIdentifier(targetField.getName, SqlParserPos.ZERO)
        throw newValidationError(id, RESOURCE.columnNotNullable(targetField.getName))
      }
    }

    validateColumnCount(sqlInsert.getSource, targetColumns.size)
  }

  /**
   * Checks that the source produces one column per listed target column where this can be decided
   * without validating the source, i.e. for a SELECT without a star and for VALUES. Other sources
   * are checked after validation.
   */
  private def validateColumnCount(source: SqlNode, expectedCount: Int): Unit = {
    source match {
      case select: SqlSelect if !select.getSelectList.exists(isStar) =>
        if (select.getSelectList.size != expectedCount) {
          throw newValidationError(select, RESOURCE.columnCountMismatch())
        }
      case values: SqlCall if values.getKind == SqlKind.VALUES =>
        values.getOperandList.foreach {
          case row: SqlCall if row.getOperandList.size != expectedCount =>
            throw newValidationError(values, RESOURCE.columnCountMismatch())
          case _ =>
        }
      case _ =>
    }
  }

  private def isStar(node: SqlNode): Boolean = node match {
    case id: SqlIdentifier => id.isStar
    case _ => false
  }

  /**
   * Derives a physical row-type for INSERT and UPDATE operations.
   *
   * <p>This code snippet is almost inspired by
   * [[org.apache.calcite.sql.validate.SqlValidatorImpl#createTargetRowType]]. It is the best that
   * the logic can be merged into Apache Calcite, but this needs time.
   *
   * @param typeFactory
   *   TypeFactory
   * @param table
   *   Target table for INSERT/UPDATE
   * @return
   *   Rowtype
   */
  private def createTargetRowType(
      typeFactory: RelDataTypeFactory,
      table: SqlValidatorTable): RelDataType = {
    table.unwrap(classOf[FlinkPreparingTableBase]) match {
      case t: CatalogSourceTable =>
        val schema = t.getCatalogTable.getSchema
        typeFactory.asInstanceOf[FlinkTypeFactory].buildPersistedRelNodeRowType(schema)
      case t: LegacyCatalogSourceTable[_] =>
        val schema = t.catalogTable.getSchema
        typeFactory.asInstanceOf[FlinkTypeFactory].buildPersistedRelNodeRowType(schema)
      case _ =>
        table.getRowType
    }
  }

  /** Check whether the field is valid. * */
  private def validateField(
      tester: Function[Integer, Boolean],
      id: SqlIdentifier,
      targetField: RelDataTypeField): Unit = {
    if (targetField == null) {
      throw newValidationError(id, RESOURCE.unknownTargetColumn(id.toString))
    }
    if (!tester.apply(targetField.getIndex)) {
      throw newValidationError(id, RESOURCE.duplicateTargetColumn(targetField.getName))
    }
  }

  private def newValidationError(
      node: SqlNode,
      e: Resources.ExInst[SqlValidatorException]): CalciteContextException = {
    assert(node != null)
    val pos = node.getParserPosition
    SqlUtil.newContextException(pos, e)
  }

  private def validateUnsupportedCompositeColumn(id: SqlIdentifier): Unit = {
    assert(id != null)
    if (!id.isSimple) {
      val pos = id.getParserPosition
      // TODO no suitable error message from current CalciteResource, just use this one temporarily,
      // we will remove this after composite column name is supported.
      throw SqlUtil.newContextException(pos, RESOURCE.unknownTargetColumn(id.toString))
    }
  }
}
