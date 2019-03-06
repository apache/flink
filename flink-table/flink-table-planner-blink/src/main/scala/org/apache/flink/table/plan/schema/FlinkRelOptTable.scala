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

import org.apache.flink.table.plan.stats.FlinkStatistic

import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.enumerable.EnumerableTableScan
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.plan.{RelOptCluster, RelOptSchema}
import org.apache.calcite.prepare.Prepare.AbstractPreparingTable
import org.apache.calcite.prepare.{CalcitePrepareImpl, RelOptTableImpl}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema._
import org.apache.calcite.sql.SqlAccessType
import org.apache.calcite.sql.validate.{SqlModality, SqlMonotonicity}
import org.apache.calcite.sql2rel.InitializerContext
import org.apache.calcite.util.ImmutableBitSet

import java.util.{List => JList}

import scala.collection.JavaConversions._

/**
  * [[FlinkRelOptTable]] wraps a [[FlinkTable]]
  *
  * @param schema  the [[RelOptSchema]] this table belongs to
  * @param rowType the type of rows returned by this table
  * @param names   the identifier for this table. The identifier must be unique with
  *                respect to the Connection producing this table.
  * @param table   wrapped flink table
  */
class FlinkRelOptTable protected(
    schema: RelOptSchema,
    rowType: RelDataType,
    names: JList[String],
    table: FlinkTable)
  extends AbstractPreparingTable {

  // Default value of rowCount if there is no available stats.
  // Sets a bigger default value to avoid broadcast join.
  val DEFAULT_ROWCOUNT: Double = 1E8

  def copy(newTable: FlinkTable, newRowType: RelDataType): FlinkRelOptTable =
    new FlinkRelOptTable(schema, newRowType, names, newTable)

  /**
    * Obtains an identifier for this table.
    *
    * Note: the qualified names are used for computing the digest of TableScan.
    *
    * @return qualified name
    */
  override def getQualifiedName: JList[String] = names

  /**
    * Obtains the access type of the table.
    *
    * @return all access types including SELECT/UPDATE/INSERT/DELETE
    */
  override def getAllowedAccess: SqlAccessType = SqlAccessType.ALL

  override def unwrap[T](clazz: Class[T]): T = {
    if (clazz.isInstance(this)) {
      clazz.cast(this)
    } else if (clazz.isInstance(table)) {
      clazz.cast(table)
    } else {
      null.asInstanceOf[T]
    }
  }

  /**
    * Returns true if the given modality is supported, else false.
    */
  override def supportsModality(modality: SqlModality): Boolean = modality match {
    case SqlModality.STREAM =>
      table.isInstanceOf[StreamableTable]
    case _ =>
      !table.isInstanceOf[StreamableTable]
  }

  /**
    * Returns the type of rows returned by this table.
    */
  override def getRowType: RelDataType = rowType

  /**
    * Obtains whether a given column is monotonic.
    *
    * @param columnName column name
    * @return true if the given column is monotonic
    */
  override def getMonotonicity(columnName: String): SqlMonotonicity = {
    val columnIdx = rowType.getFieldNames.indexOf(columnName)
    if (columnIdx >= 0) {
      for (collation: RelCollation <- table.getStatistic.getCollations) {
        val fieldCollation: RelFieldCollation = collation.getFieldCollations.get(0)
        if (fieldCollation.getFieldIndex == columnIdx) {
          return fieldCollation.direction.monotonicity
        }
      }
    }
    SqlMonotonicity.NOT_MONOTONIC
  }

  /**
    * Returns flink table statistics.
    */
  def getFlinkStatistic: FlinkStatistic = {
    if (table.getStatistic != null) {
      table.getStatistic
    } else {
      FlinkStatistic.UNKNOWN
    }
  }

  /**
    * Returns an estimate of the number of rows in the table.
    */
  override def getRowCount: Double =
    if (table.getStatistic != null) {
      table.getStatistic match {
        case stats: FlinkStatistic =>
          if (stats.getRowCount != null) {
            stats.getRowCount
          } else {
            DEFAULT_ROWCOUNT
          }
        case _ => throw new AssertionError
      }
    } else {
      DEFAULT_ROWCOUNT
    }

  /**
    * Converts this table into a [[RelNode]] relational expression.
    *
    * @return the RelNode converted from this table
    */
  override def toRel(context: ToRelContext): RelNode = {
    val cluster: RelOptCluster = context.getCluster
    if (table.isInstanceOf[TranslatableTable]) {
      table.asInstanceOf[TranslatableTable].toRel(context, this)
    } else if (Hook.ENABLE_BINDABLE.get(false)) {
      LogicalTableScan.create(cluster, this)
    } else if (CalcitePrepareImpl.ENABLE_ENUMERABLE) {
      EnumerableTableScan.create(cluster, this)
    } else {
      throw new AssertionError
    }
  }

  /**
    * Returns the [[RelOptSchema]] this table belongs to.
    */
  override def getRelOptSchema: RelOptSchema = schema

  /**
    * Returns whether the given columns are a key or a superset of a unique key
    * of this table.
    *
    * Note: Return true means TRUE. However return false means FALSE or NOT KNOWN.
    * It's better to use [[org.apache.calcite.rel.metadata.RelMetadataQuery]].areRowsUnique to
    * distinguish FALSE with NOT KNOWN.
    *
    * @param columns Ordinals of key columns
    * @return if the input columns bits represents a unique column set; false if not (or
    *         if no metadata is available)
    */
  @Deprecated
  override def isKey(columns: ImmutableBitSet): Boolean = false

  /**
    * Returns the referential constraints existing for this table. These constraints
    * are represented over other tables using [[RelReferentialConstraint]] nodes.
    */
  override def getReferentialConstraints: JList[RelReferentialConstraint] = {
    if (table.getStatistic != null) {
      table.getStatistic.getReferentialConstraints
    } else {
      ImmutableList.of()
    }
  }


  /**
    * Returns a description of the physical ordering (or orderings) of the rows
    * returned from this table.
    *
    * @see [[org.apache.calcite.rel.metadata.RelMetadataQuery#collations(RelNode)]]
    */
  override def getCollationList: JList[RelCollation] = {
    if (table.getStatistic != null) {
      table.getStatistic.getCollations
    } else {
      ImmutableList.of()
    }
  }

  /**
    * Returns a description of the physical distribution of the rows
    * in this table.
    *
    * @see [[org.apache.calcite.rel.metadata.RelMetadataQuery#distribution(RelNode)]]
    */
  override def getDistribution: RelDistribution = {
    if (table.getStatistic != null) {
      table.getStatistic.getDistribution
    } else {
      null
    }
  }

  /**
    * Generates code for this table, which is not supported now.
    *
    * @param clazz The desired collection class; for example [[org.apache.calcite.linq4j.Queryable]]
    */
  override def getExpression(clazz: java.lang.Class[_]): Expression =
    throw new UnsupportedOperationException

  /**
    * Obtains whether the ordinal column has a default value, which is not supported now.
    *
    * @param rowType            rowType of field
    * @param ordinal            index of the given column
    * @param initializerContext the context for
    *                           [[org.apache.calcite.sql2rel.InitializerExpressionFactory]] methods
    * @return true if the column has a default value
    */
  override def columnHasDefaultValue(
      rowType: RelDataType,
      ordinal: Int,
      initializerContext: InitializerContext): Boolean = false

  override def getColumnStrategies: JList[ColumnStrategy] = RelOptTableImpl.columnStrategies(this)

  override def extend(extendedTable: Table) =
    throw new RuntimeException("Extending column not supported")

}

object FlinkRelOptTable {

  def create(schema: RelOptSchema,
      rowType: RelDataType,
      names: JList[String],
      table: FlinkTable): FlinkRelOptTable =
    new FlinkRelOptTable(schema, rowType, names, table)

}
