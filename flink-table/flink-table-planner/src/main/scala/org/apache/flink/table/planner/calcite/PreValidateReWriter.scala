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

import org.apache.flink.sql.parser.`type`.SqlMapTypeNameSpec
import org.apache.flink.sql.parser.SqlProperty
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.sql.parser.dql.SqlRichExplain
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.calcite.PreValidateReWriter.{appendPartitionAndNullsProjects, notSupported}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.schema.{CatalogSourceTable, FlinkPreparingTableBase, LegacyCatalogSourceTable}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.util.Preconditions
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.runtime.{CalciteContextException, Resources}
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.sql.{SqlCall, SqlDataTypeSpec, SqlIdentifier, SqlKind, SqlLiteral, SqlNode, SqlNodeList, SqlOrderBy, SqlSelect, SqlTableRef, SqlUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.validate.{SqlValidatorException, SqlValidatorTable, SqlValidatorUtil}
import org.apache.calcite.util.Static.RESOURCE

import javax.annotation.Nullable

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
 * Implements [[org.apache.calcite.sql.util.SqlVisitor]] interface to do some rewrite work before
 * sql node validation.
 */
class PreValidateReWriter(
    val validator: FlinkCalciteSqlValidator,
    val typeFactory: RelDataTypeFactory)
  extends SqlBasicVisitor[Unit] {
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
          val newSource =
            appendPartitionAndNullsProjects(r, validator, typeFactory, call, r.getStaticPartitions)
          r.setOperand(2, newSource)
        case source => throw new ValidationException(notSupported(source))
      }
    }
  }
}

object PreValidateReWriter {

  // ~ Tools ------------------------------------------------------------------

  private def notSupported(source: SqlNode): String = {
    s"INSERT INTO <table> PARTITION [(COLUMN LIST)] statement only support " +
      s"SELECT, VALUES, SET_QUERY AND ORDER BY clause for now, '$source' is not supported yet."
  }

  /**
   * This refers to a single target column specified in the 'insert into' clause.
   * @param path
   *   The access path for this column starts from its index in the Table Schema. If the column is
   *   nested, it will have additional pointers following it.
   * @param idxInTargetColumnList
   *   The index of this column in the 'insert into (col1, col2)' clause.
   * @param field
   *   The [[RelDataTypeField]] of this column.
   */
  case class TargetColumn(path: util.List[Int], idxInTargetColumnList: Int, field: RelDataTypeField)

  /**
   * Append the static partitions and unspecified columns to the data source projection list. The
   * columns are appended to the corresponding positions.
   *
   * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt) whose partition columns
   * are (&lt;a&gt;, &lt;c&gt;), and got a query <blockquote><pre> insert into A partition(a='11',
   * c='22') select b from B </pre></blockquote> The query would be rewritten to: <blockquote><pre>
   * insert into A partition(a='11', c='22') select cast('11' as tpe1), b, cast('22' as tpe2) from B
   * </pre></blockquote> Where the "tpe1" and "tpe2" are data types of column a and c of target
   * table A.
   *
   * <p>If we have a table A with schema (&lt;a&gt;, &lt;b&gt;, &lt;c&gt), and got a query
   * <blockquote><pre> insert into A (a, b) select a, b from B </pre></blockquote> The query would
   * be rewritten to: <blockquote><pre> insert into A select a, b, cast(null as tpeC) from B
   * </pre></blockquote> Where the "tpeC" is data type of column c for target table A.
   *
   * @param sqlInsert
   *   RichSqlInsert instance
   * @param validator
   *   Validator
   * @param typeFactory
   *   type factory
   * @param source
   *   Source to rewrite
   * @param partitions
   *   Static partition statements
   */
  def appendPartitionAndNullsProjects(
      sqlInsert: RichSqlInsert,
      validator: FlinkCalciteSqlValidator,
      typeFactory: RelDataTypeFactory,
      source: SqlCall,
      partitions: SqlNodeList): SqlCall = {
    val calciteCatalogReader = validator.getCatalogReader.unwrap(classOf[CalciteCatalogReader])
    val names = sqlInsert.getTargetTable match {
      case si: SqlIdentifier => si.names
      case st: SqlTableRef => st.getOperandList.get(0).asInstanceOf[SqlIdentifier].names
    }
    val table = calciteCatalogReader.getTable(names)
    if (table == null) {
      // There is no table exists in current catalog,
      // just skip to let other validation error throw.
      return source
    }
    val targetRowType = createTargetRowType(typeFactory, table)
    // validate partition fields first.
    val assignedFields = new util.LinkedHashMap[Integer, SqlNode]
    val partialSupplierFunction = new util.LinkedHashMap[Integer, util.List[SqlNode] => SqlNode]
    val relOptTable = table match {
      case t: RelOptTable => t
      case _ => null
    }
    for (node <- partitions.getList) {
      val sqlProperty = node.asInstanceOf[SqlProperty]
      val id = sqlProperty.getKey
      validateUnsupportedCompositeColumn(id)
      val targetField = SqlValidatorUtil.getTargetField(
        targetRowType,
        typeFactory,
        id,
        calciteCatalogReader,
        relOptTable)
      validateField(idx => !assignedFields.contains(idx), id, targetField)
      val value = sqlProperty.getValue.asInstanceOf[SqlLiteral]
      assignedFields.put(
        targetField.getIndex,
        maybeCast(value, value.createSqlType(typeFactory), targetField.getType, typeFactory))
    }

    // validate partial insert columns.

    // the columnList may reorder fields (compare with fields of sink)
    val targetPosition = new util.ArrayList[Int]()

    if (sqlInsert.getTargetColumnList != null) {
      val targetFieldsAccess = new util.HashSet[util.List[Int]]
      val targetColumns =
        sqlInsert.getTargetColumnList.getList.zipWithIndex.map {
          case (id, idx) =>
            val identifier = id.asInstanceOf[SqlIdentifier]
            val (path, field) = JavaScalaConversionUtil.toScala(
              calciteCatalogReader
                .nameMatcher()
                .asInstanceOf[FlinkSqlNameMatcher]
                .field(targetRowType, identifier))
            val targetColumn = TargetColumn(path.map(x => x.toInt), idx, field)
            validateField(
              _ => targetFieldsAccess.add(targetColumn.path),
              id.asInstanceOf[SqlIdentifier],
              targetColumn.field)
            targetColumn
        }

      val partitionColumns =
        partitions.getList
          .map(
            property =>
              SqlValidatorUtil.getTargetField(
                targetRowType,
                typeFactory,
                property.asInstanceOf[SqlProperty].getKey,
                calciteCatalogReader,
                relOptTable))

      for (targetField <- targetRowType.getFieldList) {
        if (!partitionColumns.contains(targetField)) {
          val targetFieldIndex = targetField.getIndex
          // target columns access from this index
          val columns = targetColumns.filter(t => t.path(0) == targetFieldIndex)
          // this targetField is not used
          val id = new SqlIdentifier(targetField.getName, SqlParserPos.ZERO)
          if (columns.isEmpty) {
            // padding null
            checkNullability(targetField, id)
            validateField(idx => !assignedFields.contains(idx), id, targetField)
            assignedFields.put(
              targetFieldIndex,
              maybeCast(
                SqlLiteral.createNull(SqlParserPos.ZERO),
                typeFactory.createUnknownType(),
                targetField.getType,
                typeFactory))
          } else {
            // nested type
            if (targetField.getType.isStruct) {
              // padding partial null for nested type
              validateField(idx => !partialSupplierFunction.contains(idx), id, targetField)
              partialSupplierFunction.put(
                targetFieldIndex,
                buildRowWithPartialNull(
                  columns.toList,
                  targetField,
                  typeFactory,
                  _: util.List[SqlNode]))
            } else {
              // not nested type
              Preconditions.checkArgument(columns.size == 1)
              // handle reorder
              targetPosition.add(columns.get(0).idxInTargetColumnList)
            }
          }
        }
      }
    }

    rewriteSqlCall(
      validator,
      source,
      targetRowType,
      assignedFields,
      partialSupplierFunction,
      targetPosition)
  }

  private def checkNullability(field: RelDataTypeField, @Nullable id: SqlNode): Unit = {
    // for the nested field, we can not get
    val node = if (id == null) {
      new SqlIdentifier(new util.ArrayList[String](), SqlParserPos.ZERO)
    } else {
      id
    }
    if (!field.getType.isNullable) {
      throw newValidationError(node, RESOURCE.columnNotNullable(field.getName))
    }
    if (field.getType.isStruct) {
      field.getType.getFieldList.foreach(f => checkNullability(f, node))
    }
  }

  /**
   * Build up a row with partial value from the sourceNodes for one sink target field. For example:
   * If we hava a table with schema (r1 Row< a VARCHAR, b VARCHAR>, r2 Row< c BIGINT, d INT>).
   *
   * Then the following select list can be inferred based on the access path
   *
   * <li> access path ([0, 0], [1, 1]) with generate (ROW(r1.a, CAST(NULL as VARCHAR), ROW(CAST
   * (NULL as BIGINT), r2.d)) as select list <li> access path ([0], [1, 1]) with generate (r1,
   * ROW(CAST (NULL as BIGINT), r2.d)) as select list <li>...
   *
   * @param derivedTargetColumns
   *   columns in the insert into clause derived from the targetField
   * @param targetField
   *   The target field in the sink table schema
   * @param typeFactory
   *   The type factory
   * @param sourceNodes
   *   The list of the select nodes.
   * @return
   *   The single row call built from the target columns,
   */
  private def buildRowWithPartialNull(
      derivedTargetColumns: List[TargetColumn],
      targetField: RelDataTypeField,
      typeFactory: RelDataTypeFactory,
      sourceNodes: util.List[SqlNode]): SqlNode = {
    Preconditions.checkArgument(targetField.getType.isStruct)
    // access path to target column mapping
    val sep = "->"
    val mapping = derivedTargetColumns.map(c => c.path.mkString(sep) -> c).toMap

    def path_matching(accessPath: String): (Boolean, Boolean) = {
      val matched = mapping.keySet.contains(accessPath)
      val deeper = mapping.keySet.exists(k => k.startsWith(accessPath) && !k.equals(accessPath))
      (matched, deeper)
    }

    def traverse(field: RelDataTypeField, path: util.List[Int]): SqlNode = {
      path.add(field.getIndex)
      val accessPath = path.mkString(sep)
      if (field.getType.isStruct) {
        val (matched, deeper) = path_matching(accessPath)
        if (matched) {
          // get sqlNode from sql select
          val targetColumn = mapping(accessPath)
          path.remove(path.size() - 1)
          sourceNodes.get(targetColumn.idxInTargetColumnList)
        } else if (deeper) {
          val fields = field.getType.getFieldList.map(f => traverse(f, path))
          path.remove(path.size() - 1)
          FlinkSqlOperatorTable.ROW.createCall(SqlParserPos.ZERO, fields.toList)
        } else {
          checkNullability(field, null)
          maybeCast(
            SqlLiteral.createNull(SqlParserPos.ZERO),
            typeFactory.createUnknownType(),
            field.getType,
            typeFactory)
        }
      } else {
        val (matched, _: Boolean) = path_matching(accessPath)
        path.remove(path.size - 1)
        if (matched) {
          // get sqlNode from sqlSelect
          val targetColumn = mapping(accessPath)
          sourceNodes.get(targetColumn.idxInTargetColumnList)
        } else {
          checkNullability(field, null)
          maybeCast(
            SqlLiteral.createNull(SqlParserPos.ZERO),
            typeFactory.createUnknownType(),
            field.getType,
            typeFactory)
        }
      }
    }

    val path = new util.ArrayList[Int]()
    val name = new util.ArrayList[String]()
    path.add(targetField.getIndex)
    val (matched, _: Boolean) = path_matching(path.mkString(sep))
    if (matched) {
      sourceNodes.get(mapping(path.mkString(sep)).idxInTargetColumnList)
    } else {
      val fields = targetField.getType.getFieldList.map(field => traverse(field, path))
      FlinkSqlOperatorTable.ROW.createCall(SqlParserPos.ZERO, fields.toList)
    }
  }

  private def rewriteSqlCall(
      validator: FlinkCalciteSqlValidator,
      call: SqlCall,
      targetRowType: RelDataType,
      assignedFields: util.LinkedHashMap[Integer, SqlNode],
      partialSupplier: util.LinkedHashMap[Integer, util.List[SqlNode] => SqlNode],
      targetPosition: util.List[Int]): SqlCall = {

    def rewrite(node: SqlNode): SqlCall = {
      checkArgument(node.isInstanceOf[SqlCall], node)
      rewriteSqlCall(
        validator,
        node.asInstanceOf[SqlCall],
        targetRowType,
        assignedFields,
        partialSupplier,
        targetPosition)
    }

    call.getKind match {
      case SqlKind.SELECT =>
        rewriteSelect(
          validator,
          call.asInstanceOf[SqlSelect],
          targetRowType,
          assignedFields,
          partialSupplier,
          targetPosition)
      case SqlKind.VALUES =>
        rewriteValues(call, targetRowType, assignedFields, partialSupplier, targetPosition)
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
      partialSupplier: util.LinkedHashMap[Integer, util.List[SqlNode] => SqlNode],
      targetPosition: util.List[Int]): SqlCall = {
    // Expands the select list first in case there is a star(*).
    // Validates the select first to register the where scope.
    validator.validate(select)
    val sourceList = validator.expandStar(select.getSelectList, select, false).getList

    val fixedNodes = new util.ArrayList[SqlNode]
    val currentNodes = {
      if (targetPosition.isEmpty) {
        new util.ArrayList[SqlNode](sourceList)
      } else {
        reorder(new util.ArrayList[SqlNode](sourceList), targetPosition)
      }
    }
    (0 until targetRowType.getFieldList.length).foreach {
      idx =>
        if (assignedFields.containsKey(idx)) {
          fixedNodes.add(assignedFields.get(idx))
        } else if (partialSupplier.contains(idx)) {
          fixedNodes.add(partialSupplier.get(idx).apply(sourceList))
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
      partialSupplier: util.LinkedHashMap[Integer, util.List[SqlNode] => SqlNode],
      targetPosition: util.List[Int]): SqlCall = {
    val fixedNodes = new util.ArrayList[SqlNode]
    (0 until values.getOperandList.size()).foreach {
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
        (0 until targetRowType.getFieldList.length).foreach {
          fieldIdx =>
            if (assignedFields.containsKey(fieldIdx)) {
              fieldNodes.add(assignedFields.get(fieldIdx))
            } else if (partialSupplier.contains(fieldIdx)) {
              fieldNodes.add(partialSupplier.get(fieldIdx).apply(valueAsList))
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
   *   - sourceList(f0, f1, f2).
   *   - targetPosition(1, 2, 0).
   *   - Output(f1, f2, f0).
   *
   * @param sourceList
   *   input fields.
   * @param targetPosition
   *   reorder mapping.
   * @return
   *   reorder fields.
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

  // This code snippet is copied from the SqlValidatorImpl.
  private def maybeCast(
      node: SqlNode,
      currentType: RelDataType,
      desiredType: RelDataType,
      typeFactory: RelDataTypeFactory): SqlNode = {
    if (
      currentType == desiredType
      || (currentType.isNullable != desiredType.isNullable
        && typeFactory.createTypeWithNullability(currentType, desiredType.isNullable)
        == desiredType)
    ) {
      node
    } else {
      // See FLINK-26460 for more details
      val sqlDataTypeSpec =
        if (SqlTypeUtil.isNull(currentType) && SqlTypeUtil.isMap(desiredType)) {
          val keyType = desiredType.getKeyType
          val valueType = desiredType.getValueType
          new SqlDataTypeSpec(
            new SqlMapTypeNameSpec(
              SqlTypeUtil.convertTypeToSpec(keyType).withNullable(keyType.isNullable),
              SqlTypeUtil.convertTypeToSpec(valueType).withNullable(valueType.isNullable),
              SqlParserPos.ZERO),
            SqlParserPos.ZERO)
        } else {
          SqlTypeUtil.convertTypeToSpec(desiredType)
        }
      SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, sqlDataTypeSpec)
    }
  }
}
