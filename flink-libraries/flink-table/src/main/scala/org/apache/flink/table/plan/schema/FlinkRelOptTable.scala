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

package org.apache.flink.table.plan.schema

import java.util.{List => JList}

import com.google.common.collect.ImmutableList

import org.apache.calcite.plan.{RelOptCluster, RelOptSchema, RelOptTable}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.prepare.CalcitePrepareImpl

import org.apache.calcite.adapter.enumerable.EnumerableTableScan
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.prepare.Prepare.PreparingTable
import org.apache.calcite.rel.`type`.{RelDataTypeFactory, RelDataTypeField}
import org.apache.calcite.rel._
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.StreamableTable
import org.apache.calcite.sql.SqlAccessType
import org.apache.calcite.sql.validate.{SqlModality, SqlMonotonicity}
import org.apache.calcite.sql2rel.InitializerContext
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConverters._

/**
  * FlinkRelOptTable wraps a FlinkTable
  *
  * @param schema        the [[RelOptSchema]] this table belongs to
  * @param rowType       the type of rows returned by this table
  * @param qualifiedName the identifier for this table. The identifier must be unique with
  *                      respect to the Connection producing this table.
  * @param table         wrapped flink table
  */

class FlinkRelOptTable private(
    schema: RelOptSchema,
    rowType: RelDataType,
    qualifiedName: JList[String],
    table: Table) extends PreparingTable {

  private[this] lazy val statistic = Option(table.getStatistic)

  /**
    * Creates a copy of this Flink RelOptTable with new Flink Table and new row type.
    *
    * @param newTable    new flink table
    * @param typeFactory type factory to create new row type of new flink table
    * @return The copy of this Flink RelOptTable with new Flink table and new row type
    */
  def copy(newTable: Table, typeFactory: RelDataTypeFactory): FlinkRelOptTable = {
    val newRowType = newTable.getRowType(typeFactory)
    FlinkRelOptTable.create(schema, newRowType, qualifiedName, newTable)
  }

  /**
    * Extends a table with the given extra fields. The operation is not supported now.
    *
    * @param extendedFields
    * @return
    */
  override def extend(extendedFields: JList[RelDataTypeField]): RelOptTable =
    throw new UnsupportedOperationException

  /**
    * Obtains an identifier for this table.
    *
    * @return qualified name
    */
  override def getQualifiedName: JList[String] = qualifiedName


  /**
    * Obtains the access type of the table.
    *
    * @return access types of given table
    */
  override def getAllowedAccess: SqlAccessType = table match {
    case _: TableSinkTable[_] =>
      // support INSERT/ SELECT for TableSink now
      SqlAccessType.create("SELECT, INSERT")
    // support SELECT for TableSouceTable/ DataSetTableTable/RelTable  now
    case _: RelTable | _: FlinkTable[_] => SqlAccessType.READ_ONLY
    case _ => throw new AssertionError
  }

  override def unwrap[T](clazz: Class[T]): T = {
    if (clazz.isInstance(this)) {
      clazz.cast(this)
    } else if (clazz.isInstance(table)) {
      clazz.cast(table)
    } else {
      null.asInstanceOf[T]
    }
  }

  override def supportsModality(modality: SqlModality): Boolean =
    modality match {
      case SqlModality.STREAM =>
        table.isInstanceOf[StreamableTable]
      case _ =>
        !table.isInstanceOf[StreamableTable]
    }

  /**
    * Obtains the type of rows returned by this table.
    *
    * @return the type of rows returned by this table.
    */
  override def getRowType: RelDataType = rowType

  /**
    * Obtains whether a given column is monotonic.
    *
    * @param columnName
    * @return true if the given column is monotonic
    */
  override def getMonotonicity(columnName: String): SqlMonotonicity = {
    val columnIdx = rowType.getFieldNames.indexOf(columnName)
    assert(columnIdx >= 0)
    statistic match {
      case Some(statistic) =>
        statistic.getCollations
            .asScala
            .map(_.getFieldCollations.get(0))
            .find(_.getFieldIndex == columnIdx)
            .map(_.direction.monotonicity())
            .getOrElse(SqlMonotonicity.NOT_MONOTONIC)
      case _ => SqlMonotonicity.NOT_MONOTONIC
    }

  }

  /**
    * Obtains an estimate of the number of rows in the table.
    *
    * @return the number of rows in the table
    */
  override def getRowCount: java.lang.Double = statistic.map(_.getRowCount).getOrElse(100D)

  /**
    * Converts this table into a RelNode relational expression.
    *
    * @param context
    * @return
    */
  override def toRel(context: ToRelContext): RelNode = {
    val cluster: RelOptCluster = context.getCluster
    table match {
      case t: RelTable => t.toRel(context, this)
      case _ =>
        if (Hook.ENABLE_BINDABLE.get(false)) {
          LogicalTableScan.create(cluster, this)
        } else if (CalcitePrepareImpl.ENABLE_ENUMERABLE) {
          EnumerableTableScan.create(cluster, this)
        } else {
          throw new AssertionError
        }
    }
  }

  /**
    * Obtains the [[RelOptSchema]] this table belongs to.
    *
    * @return the [[RelOptSchema]] this table belongs to
    */
  override def getRelOptSchema: RelOptSchema = schema

  /**
    * Returns whether the given columns are a key or a superset of a unique key
    * of this table.
    *
    * @param columns Ordinals of key columns
    * @return true if the given columns are a key or a superset of a key
    */
  override def isKey(columns: ImmutableBitSet): Boolean =
    statistic.map(_.isKey(columns)).getOrElse(false)

  /**
    * Obtains the referential constraints existing for this table.
    *
    * @return the referential constraints existing for this table
    */
  override def getReferentialConstraints: JList[RelReferentialConstraint] =
    statistic.map(_.getReferentialConstraints).getOrElse(ImmutableList.of())


  /**
    * Obtains a description of the physical ordering (or orderings) of the rows
    * returned from this table.
    *
    * @return the ordering properties of the table
    */
  override def getCollationList: JList[RelCollation] =
    statistic.map(_.getCollations).getOrElse(ImmutableList.of())

  /**
    * Obtains a description of the physical distribution of the rows
    * in this table.
    *
    * @return the distribution properties of the table
    */
  override def getDistribution: RelDistribution =
    statistic.map(_.getDistribution).getOrElse(RelDistributionTraitDef.INSTANCE.getDefault)

  /**
    * Generates code for this table. The operation is not supported now.
    *
    * @param clazz The desired collection class; for example { @code Queryable}
    * @return
    */
  override def getExpression(clazz: java.lang.Class[_]): Expression =
    throw new UnsupportedOperationException

  /**
    * Obtains whether the ordinal column has a default value.
    *
    * @param rowType
    * @param ordinal
    * @param initializerContext
    * @return true if the column has a default value
    */
  override def columnHasDefaultValue(
      rowType: RelDataType,
      ordinal: Int,
      initializerContext: InitializerContext): Boolean = false
}

object FlinkRelOptTable {

  def create(schema: RelOptSchema,
      rowType: RelDataType,
      names: JList[String],
      table: Table): FlinkRelOptTable =
    new FlinkRelOptTable(schema, rowType, names, table)

}
