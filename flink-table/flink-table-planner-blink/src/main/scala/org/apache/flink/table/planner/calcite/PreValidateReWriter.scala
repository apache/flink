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
import org.apache.flink.table.planner.calcite.PreValidateReWriter.appendPartitionProjects

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.runtime.{CalciteContextException, Resources}
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.validate.{SqlValidatorException, SqlValidatorTable, SqlValidatorUtil}
import org.apache.calcite.sql.{SqlCall, SqlIdentifier, SqlLiteral, SqlNode, SqlNodeList, SqlSelect, SqlUtil}
import org.apache.calcite.util.Static.RESOURCE

import java.util

import scala.collection.JavaConversions._

/** Implements [[org.apache.calcite.sql.util.SqlVisitor]]
  * interface to do some rewrite work before sql node validation. */
class PreValidateReWriter(
    val catalogReader: CalciteCatalogReader,
    val typeFactory: RelDataTypeFactory) extends SqlBasicVisitor[Unit] {
  override def visit(call: SqlCall): Unit = {
    call match {
      case r: RichSqlInsert if r.getStaticPartitions.nonEmpty
        && r.getSource.isInstanceOf[SqlSelect] =>
        appendPartitionProjects(r, catalogReader, typeFactory,
          r.getSource.asInstanceOf[SqlSelect], r.getStaticPartitions)
      case _ =>
    }
  }
}

object PreValidateReWriter {
  //~ Tools ------------------------------------------------------------------
  /**
    * Append the static partitions to the data source projection list. The columns are appended to
    * the corresponding positions.
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
    * @param sqlInsert            RichSqlInsert instance
    * @param calciteCatalogReader catalog reader
    * @param typeFactory          type factory
    * @param select               Source sql select
    * @param partitions           Static partition statements
    */
  def appendPartitionProjects(sqlInsert: RichSqlInsert,
      calciteCatalogReader: CalciteCatalogReader,
      typeFactory: RelDataTypeFactory,
      select: SqlSelect,
      partitions: SqlNodeList): Unit = {
    val names = sqlInsert.getTargetTable.asInstanceOf[SqlIdentifier].names
    val table = calciteCatalogReader.getTable(names)
    if (table == null) {
      // There is no table exists in current catalog,
      // just skip to let other validation error throw.
      return
    }
    val targetRowType = createTargetRowType(typeFactory,
      calciteCatalogReader, table, sqlInsert.getTargetColumnList)
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
    val currentNodes = new util.ArrayList[SqlNode](select.getSelectList.getList)
    val fixedNodes = new util.ArrayList[SqlNode]
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
  }

  /**
    * Derives a row-type for INSERT and UPDATE operations.
    *
    * <p>This code snippet is almost inspired by
    * [[org.apache.calcite.sql.validate.SqlValidatorImpl#createTargetRowType]].
    * It is the best that the logic can be merged into Apache Calcite,
    * but this needs time.
    *
    * @param typeFactory      TypeFactory
    * @param catalogReader    CalciteCatalogReader
    * @param table            Target table for INSERT/UPDATE
    * @param targetColumnList List of target columns, or null if not specified
    * @return Rowtype
    */
  private def createTargetRowType(
      typeFactory: RelDataTypeFactory,
      catalogReader: CalciteCatalogReader,
      table: SqlValidatorTable,
      targetColumnList: SqlNodeList): RelDataType = {
    val baseRowType = table.getRowType
    if (targetColumnList == null) return baseRowType
    val fields = new util.ArrayList[util.Map.Entry[String, RelDataType]]
    val assignedFields = new util.HashSet[Integer]
    val relOptTable = table match {
      case t: RelOptTable => t
      case _ => null
    }
    for (node <- targetColumnList) {
      val id = node.asInstanceOf[SqlIdentifier]
      val targetField = SqlValidatorUtil.getTargetField(baseRowType,
        typeFactory, id, catalogReader, relOptTable)
      validateField(assignedFields.add, id, targetField)
      fields.add(targetField)
    }
    typeFactory.createStructType(fields)
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
