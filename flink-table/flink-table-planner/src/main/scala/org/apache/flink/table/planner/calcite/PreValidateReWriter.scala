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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.calcite.PreValidateReWriter.{appendPartitionAndNullsProjects, notSupported}
import org.apache.flink.table.planner.plan.schema.{CatalogSourceTable, FlinkPreparingTableBase, LegacyCatalogSourceTable}
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.runtime.{CalciteContextException, Resources}
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.validate.{SqlValidatorException, SqlValidatorTable, SqlValidatorUtil}
import org.apache.calcite.sql.{SqlCall, SqlIdentifier, SqlKind, SqlLiteral, SqlNode, SqlNodeList, SqlOrderBy, SqlSelect, SqlUtil}
import org.apache.calcite.util.Static.RESOURCE

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/** Implements [[org.apache.calcite.sql.util.SqlVisitor]]
  * interface to do some rewrite work before sql node validation. */
class PreValidateReWriter(
    val validator: FlinkCalciteSqlValidator,
    val typeFactory: RelDataTypeFactory) extends SqlBasicVisitor[Unit] {
  override def visit(call: SqlCall): Unit = {
    call match {
      case e: SqlRichExplain =>
        e.getStatement match {
          case r: RichSqlInsert => rewriteInsert(r)
          case _ => // do nothing
        }
      case r: RichSqlInsert => rewriteInsert(r)
      case _ => // do nothing
    }
  }

  private def rewriteInsert(r: RichSqlInsert): Unit = {
    if (r.getStaticPartitions.nonEmpty || r.getTargetColumnList != null) {
      r.getSource match {
        case call: SqlCall =>
          val newSource = appendPartitionAndNullsProjects(
            r, validator, typeFactory, call, r.getStaticPartitions)
          r.setOperand(2, newSource)
        case source => throw new ValidationException(notSupported(source))
      }
    }
  }
}

object PreValidateReWriter {

  //~ Tools ------------------------------------------------------------------

  private def notSupported(source: SqlNode): String = {
    s"INSERT INTO <table> PARTITION [(COLUMN LIST)] statement only support " +
        s"SELECT, VALUES, SET_QUERY AND ORDER BY clause for now, '$source' is not supported yet."
  }

  /**
    * Append the static partitions and unspecified columns to the data source projection list.
    * The columns are appended to the corresponding positions.
    *
    * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt) whose
    * partition columns are (&lt;a&gt;, &lt;c&gt;), and got a query
    * <blockquote><pre>
    * insert into A partition(a='11', c='22')
    * select b from B
    * </pre></blockquote>
    * The query would be rewritten to:
    * <blockquote><pre>
    * insert into A partition(a='11', c='22')
    * select cast('11' as tpe1), b, cast('22' as tpe2) from B
    * </pre></blockquote>
    * Where the "tpe1" and "tpe2" are data types of column a and c of target table A.
    *
    * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt), and got a query
    * <blockquote><pre>
    * insert into A (a, b)
    * select a, b from B
    * </pre></blockquote>
    * The query would be rewritten to:
    * <blockquote><pre>
    * insert into A
    * select a, b, cast(null as tpeC) from B
    * </pre></blockquote>
    * Where the "tpeC" is data type of column c for target table A.
    *
    * @param sqlInsert            RichSqlInsert instance
    * @param validator            Validator
    * @param typeFactory          type factory
    * @param source               Source to rewrite
    * @param partitions           Static partition statements
    */
  def appendPartitionAndNullsProjects(sqlInsert: RichSqlInsert,
      validator: FlinkCalciteSqlValidator,
      typeFactory: RelDataTypeFactory,
      source: SqlCall,
      partitions: SqlNodeList): SqlCall = {
    val calciteCatalogReader = validator.getCatalogReader.unwrap(classOf[CalciteCatalogReader])
    val names = sqlInsert.getTargetTable.asInstanceOf[SqlIdentifier].names
    val table = calciteCatalogReader.getTable(names)
    if (table == null) {
      // There is no table exists in current catalog,
      // just skip to let other validation error throw.
      return source
    }
    val targetRowType = createTargetRowType(typeFactory, table)
    // validate partition fields first.
    val assignedFields = new util.LinkedHashMap[Integer, SqlNode]
    val relOptTable = table match {
      case t: RelOptTable => t
      case _ => null
    }
    for (node <- partitions.getList) {
      val sqlProperty = node.asInstanceOf[SqlProperty]
      val id = sqlProperty.getKey
      val targetField = SqlValidatorUtil.getTargetField(targetRowType,
        typeFactory, id, calciteCatalogReader, relOptTable)
      validateField(idx => !assignedFields.contains(idx), id, targetField)
      val value = sqlProperty.getValue.asInstanceOf[SqlLiteral]
      assignedFields.put(targetField.getIndex,
        maybeCast(value, value.createSqlType(typeFactory), targetField.getType, typeFactory))
    }

    // validate partial insert columns.

    // the columnList may reorder fields (compare with fields of sink)
    val targetPosition = new util.ArrayList[Int]()

    if (sqlInsert.getTargetColumnList != null) {
      val targetFields = new util.HashSet[Integer]
      val targetColumns =
        sqlInsert
          .getTargetColumnList
          .getList
          .map(id => {
            val targetField = SqlValidatorUtil.getTargetField(
              targetRowType, typeFactory, id.asInstanceOf[SqlIdentifier],
              calciteCatalogReader, relOptTable)
            validateField(targetFields.add, id.asInstanceOf[SqlIdentifier], targetField)
            targetField
          })

      val partitionColumns =
        partitions
          .getList
          .map(property =>
            SqlValidatorUtil.getTargetField(
              targetRowType, typeFactory, property.asInstanceOf[SqlProperty].getKey,
              calciteCatalogReader, relOptTable))

      for (targetField <- targetRowType.getFieldList) {
        if (!partitionColumns.contains(targetField)) {
          if (!targetColumns.contains(targetField)) {
            // padding null
            val id = new SqlIdentifier(targetField.getName, SqlParserPos.ZERO)
            if (!targetField.getType.isNullable) {
              throw newValidationError(id, RESOURCE.columnNotNullable(targetField.getName))
            }
            validateField(idx => !assignedFields.contains(idx), id, targetField)
            assignedFields.put(targetField.getIndex,
              maybeCast(
                SqlLiteral.createNull(SqlParserPos.ZERO),
                typeFactory.createUnknownType(),
                targetField.getType,
                typeFactory))
          } else {
            // handle reorder
            targetPosition.add(targetColumns.indexOf(targetField))
          }
        }
      }
    }

    rewriteSqlCall(validator, source, targetRowType, assignedFields, targetPosition)
  }

  private def rewriteSqlCall(
      validator: FlinkCalciteSqlValidator,
      call: SqlCall,
      targetRowType: RelDataType,
      assignedFields: util.LinkedHashMap[Integer, SqlNode],
      targetPosition: util.List[Int]): SqlCall = {

    def rewrite(node: SqlNode): SqlCall = {
      checkArgument(node.isInstanceOf[SqlCall], node)
      rewriteSqlCall(
        validator,
        node.asInstanceOf[SqlCall],
        targetRowType,
        assignedFields,
        targetPosition)
    }

    call.getKind match {
      case SqlKind.SELECT =>
        rewriteSelect(
          validator, call.asInstanceOf[SqlSelect], targetRowType, assignedFields, targetPosition)
      case SqlKind.VALUES =>
        rewriteValues(call, targetRowType, assignedFields, targetPosition)
      case kind if SqlKind.SET_QUERY.contains(kind) =>
        call.getOperandList.zipWithIndex.foreach {
          case (operand, index) => call.setOperand(index, rewrite(operand))
        }
        call
      case SqlKind.ORDER_BY =>
        val operands = call.getOperandList
        new SqlOrderBy(
          call.getParserPosition,
          rewrite(operands.get(0)),
          operands.get(1).asInstanceOf[SqlNodeList],
          operands.get(2),
          operands.get(3))
      // Not support:
      // case SqlKind.WITH =>
      // case SqlKind.EXPLICIT_TABLE =>
      case _ => throw new ValidationException(notSupported(call))
    }
  }

  private def rewriteSelect(
      validator: FlinkCalciteSqlValidator,
      select: SqlSelect,
      targetRowType: RelDataType,
      assignedFields: util.LinkedHashMap[Integer, SqlNode],
      targetPosition: util.List[Int]): SqlCall = {
    // Expands the select list first in case there is a star(*).
    // Validates the select first to register the where scope.
    validator.validate(select)
    val sourceList = validator.expandStar(select.getSelectList, select, false).getList

    val fixedNodes = new util.ArrayList[SqlNode]
    val currentNodes =
      if (targetPosition.isEmpty) {
        new util.ArrayList[SqlNode](sourceList)
      } else {
        reorder(new util.ArrayList[SqlNode](sourceList), targetPosition)
      }
    0 until targetRowType.getFieldList.length foreach {
      idx =>
        if (assignedFields.containsKey(idx)) {
          fixedNodes.add(assignedFields.get(idx))
        } else if (currentNodes.size() > 0) {
          fixedNodes.add(currentNodes.remove(0))
        }
    }
    // Although it is error case, we still append the old remaining
    // projection nodes to new projection.
    if (currentNodes.size > 0) {
      fixedNodes.addAll(currentNodes)
    }
    select.setSelectList(new SqlNodeList(fixedNodes, select.getSelectList.getParserPosition))
    select
  }

  private def rewriteValues(
      values: SqlCall,
      targetRowType: RelDataType,
      assignedFields: util.LinkedHashMap[Integer, SqlNode],
      targetPosition: util.List[Int]): SqlCall = {
    val fixedNodes = new util.ArrayList[SqlNode]
    0 until values.getOperandList.size() foreach {
      valueIdx =>
        val value = values.getOperandList.get(valueIdx)
        val valueAsList = if (value.getKind == SqlKind.ROW) {
          value.asInstanceOf[SqlCall].getOperandList
        } else {
          Collections.singletonList(value)
        }
        val currentNodes =
          if (targetPosition.isEmpty) {
            new util.ArrayList[SqlNode](valueAsList)
          } else {
            reorder(new util.ArrayList[SqlNode](valueAsList), targetPosition)
          }
        val fieldNodes = new util.ArrayList[SqlNode]
        0 until targetRowType.getFieldList.length foreach {
          fieldIdx =>
            if (assignedFields.containsKey(fieldIdx)) {
              fieldNodes.add(assignedFields.get(fieldIdx))
            } else if (currentNodes.size() > 0) {
              fieldNodes.add(currentNodes.remove(0))
            }
        }
        // Although it is error case, we still append the old remaining
        // value items to new item list.
        if (currentNodes.size > 0) {
          fieldNodes.addAll(currentNodes)
        }
        fixedNodes.add(SqlStdOperatorTable.ROW.createCall(value.getParserPosition, fieldNodes))
    }
    SqlStdOperatorTable.VALUES.createCall(values.getParserPosition, fixedNodes)
  }

  /**
   * Reorder sourceList to targetPosition. For example:
   * - sourceList(f0, f1, f2).
   * - targetPosition(1, 2, 0).
   * - Output(f1, f2, f0).
   *
   * @param sourceList input fields.
   * @param targetPosition reorder mapping.
   * @return reorder fields.
   */
  private def reorder(
      sourceList: util.ArrayList[SqlNode],
      targetPosition: util.List[Int]): util.ArrayList[SqlNode] = {
    new util.ArrayList[SqlNode](targetPosition.map(sourceList.get))
  }

  /**
    * Derives a physical row-type for INSERT and UPDATE operations.
    *
    * <p>This code snippet is almost inspired by
    * [[org.apache.calcite.sql.validate.SqlValidatorImpl#createTargetRowType]].
    * It is the best that the logic can be merged into Apache Calcite,
    * but this needs time.
    *
    * @param typeFactory      TypeFactory
    * @param table            Target table for INSERT/UPDATE
    * @return Rowtype
    */
  private def createTargetRowType(
      typeFactory: RelDataTypeFactory,
      table: SqlValidatorTable): RelDataType = {
    table.unwrap(classOf[FlinkPreparingTableBase]) match {
      case t: CatalogSourceTable =>
        val schema = t.getCatalogTable.getSchema
        typeFactory.asInstanceOf[FlinkTypeFactory].buildPhysicalRelNodeRowType(schema)
      case t: LegacyCatalogSourceTable[_] =>
        val schema = t.catalogTable.getSchema
        typeFactory.asInstanceOf[FlinkTypeFactory].buildPhysicalRelNodeRowType(schema)
      case _ =>
        table.getRowType
    }
  }

  /** Check whether the field is valid. **/
  private def validateField(tester: Function[Integer, Boolean],
      id: SqlIdentifier,
      targetField: RelDataTypeField): Unit = {
    if (targetField == null) {
      throw newValidationError(id, RESOURCE.unknownTargetColumn(id.toString))
    }
    if (!tester.apply(targetField.getIndex)) {
      throw newValidationError(id, RESOURCE.duplicateTargetColumn(targetField.getName))
    }
  }

  private def newValidationError(node: SqlNode,
    e: Resources.ExInst[SqlValidatorException]): CalciteContextException = {
    assert(node != null)
    val pos = node.getParserPosition
    SqlUtil.newContextException(pos, e)
  }

  // This code snippet is copied from the SqlValidatorImpl.
  private def maybeCast(node: SqlNode,
    currentType: RelDataType,
    desiredType: RelDataType,
    typeFactory: RelDataTypeFactory): SqlNode = {
    if (currentType == desiredType
      || (currentType.isNullable != desiredType.isNullable
      && typeFactory.createTypeWithNullability(currentType, desiredType.isNullable)
      == desiredType)) {
      node
    } else {
      SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO,
        node, SqlTypeUtil.convertTypeToSpec(desiredType))
    }
  }
}
