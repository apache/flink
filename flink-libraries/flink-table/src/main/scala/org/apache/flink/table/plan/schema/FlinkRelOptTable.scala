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

import java.util.{List => JList, Set => JSet}

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.adapter.enumerable.EnumerableTableScan
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.plan.{RelOptCluster, RelOptSchema, RelOptTable}
import org.apache.calcite.prepare.Prepare.AbstractPreparingTable
import org.apache.calcite.prepare.{CalcitePrepareImpl, RelOptTableImpl}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.runtime.Hook
import org.apache.calcite.schema._
import org.apache.calcite.sql.SqlAccessType
import org.apache.calcite.sql.validate.{SqlModality, SqlMonotonicity}
import org.apache.calcite.sql2rel.{InitializerContext, NullInitializerExpressionFactory}
import org.apache.calcite.util.{ImmutableBitSet, Util}
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource

import scala.collection.JavaConversions._

/**
  * FlinkRelOptTable wraps a FlinkTable
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
    table: FlinkTable) extends AbstractPreparingTable {

  // Default value of rowCount if there is no available stats.
  // Sets a bigger default value to avoid broadcast join.
  val DEFAULT_ROWCOUNT: Double = 1E8

  // unique keySets of current table.
  lazy val uniqueKeysSet: Option[JSet[ImmutableBitSet]] = if (table.getStatistic != null) {
    table.getStatistic match {
      case fStats: FlinkStatistic if fStats.getUniqueKeys == null => None
      case fStats: FlinkStatistic =>
        val uniqueKeys = fStats.getUniqueKeys
          if (uniqueKeys.isEmpty) {
            Option(ImmutableSet.of())
          } else {
            val uniqueKeysSetBuilder = ImmutableSet.builder[ImmutableBitSet]()
            for (keys <- uniqueKeys) {
              // all columns in original uniqueKey may not exist in RowType after PPD.
              val allUniqkeyColumnsExist = !keys.exists(rowType.getField(_, false, false) == null)
              // if not all columns in original uniqueKey, skip this uniqueKey
              if (allUniqkeyColumnsExist) {
                val keysPosition = keys.map(rowType.getField(_, false, false).getIndex).toArray
                uniqueKeysSetBuilder.add(ImmutableBitSet.of(keysPosition: _*))
              }
            }
            Option(uniqueKeysSetBuilder.build())
          }
      case _ => None
    }
  } else {
    None
  }

  lazy val initializerExpressionFactory = new NullInitializerExpressionFactory() {
    override def generationStrategy(table:RelOptTable, iColumn: Int):ColumnStrategy = {
      table.unwrap(classOf[CatalogCalciteTable]) match {
        case catalogTable: CatalogCalciteTable if
            !catalogTable.table.getRichTableSchema.getColumnNames.contains(
                table.getRowType.getFieldList.get(iColumn).getName) =>
          ColumnStrategy.VIRTUAL
        case _ =>
          super.generationStrategy(table, iColumn)
      }
    }
  }

  def copy(newTable: FlinkTable, newRowType: RelDataType): FlinkRelOptTable =
    new FlinkRelOptTable(schema, newRowType, names, newTable)


  /**
    * Obtains an identifier for this table.
    *
    * Note: the qualified names are used for computing the digest of TableScan.
    *
    * @return qualified name
    */
  override def getQualifiedName: JList[String] = {
    def explainSourceAsString(ts: TableSource): JList[String] = {
      val tsDigest = ts.explainSource()
      if (tsDigest.nonEmpty) {
        val builder = ImmutableList.builder[String]()
        builder.addAll(Util.skipLast(names))
        val completeTableName = s"${Util.last(names)}, source: [$tsDigest]"
        builder.add(completeTableName)
        builder.build()
      } else {
        names
      }
    }

    table match {
      // At this moment, table will always be instance of TableSourceSinkTable.
      case tsst: TableSourceSinkTable[_] if tsst.tableSourceTable.nonEmpty =>
        explainSourceAsString(tsst.tableSourceTable.get.tableSource)
      case tst: TableSourceTable =>
        explainSourceAsString(tst.tableSource)
      case _ => names
    }
  }

  /**
    * Obtains whether the table is temporal.
    *
    * @return true if the table is temporal
    */
  override def isTemporalTable: Boolean = table.isInstanceOf[TemporalTable]

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
    } else if (table.isInstanceOf[TableSourceSinkTable[_]]) {
      table.asInstanceOf[TableSourceSinkTable[_]].unwrap(clazz)
    } else if (clazz.isInstance(initializerExpressionFactory)) {
      clazz.cast(initializerExpressionFactory)
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
    * Obtains an estimate of the number of rows in the table.
    *
    * @return the number of rows in the table
    */
  override def getRowCount: Double =
    if (table.getStatistic != null) {
      table.getStatistic match {
        case stats: FlinkStatistic
          if (stats.getTableStats != null
            && stats.getTableStats.rowCount != null) => stats.getRowCount
        case stats: FlinkStatistic => DEFAULT_ROWCOUNT
        case _ => throw new AssertionError
      }
    } else {
      DEFAULT_ROWCOUNT
    }

  /**
    * Converts this table into a [[RelNode]] relational expression.
    *
    * @param context
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
    * Obtains the [[RelOptSchema]] this table belongs to.
    *
    * @return the [[RelOptSchema]] this table belongs to
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
  override def isKey(columns: ImmutableBitSet): Boolean = {
    uniqueKeysSet match {
      case Some(keysSet) if (keysSet.isEmpty) => false
      case Some(keysSet) => keysSet.exists(columns.contains)
      // We cannot judge whether the columns are unique keys if there is no statistics.
      case _ => false
    }
  }

  /**
    * Obtains the referential constraints existing for this table.
    *
    * @return the referential constraints existing for this table
    */
  override def getReferentialConstraints: JList[RelReferentialConstraint] = {
    if (table.getStatistic != null) {
      table.getStatistic.getReferentialConstraints
    } else {
      ImmutableList.of()
    }
  }


  /**
    * Obtains a description of the physical ordering (or orderings) of the rows
    * returned from this table.
    *
    * @return the ordering properties of the table
    */
  override def getCollationList: JList[RelCollation] = {
    if (table.getStatistic != null) {
      table.getStatistic.getCollations
    } else {
      ImmutableList.of()
    }
  }

  /**
    * Obtains a description of the physical distribution of the rows in this table.
    *
    * @return the distribution properties of the table
    */
  override def getDistribution: RelDistribution = {
    if (table.getStatistic != null) {
      table.getStatistic.getDistribution
    } else {
      FlinkRelDistributionTraitDef.INSTANCE.getDefault
    }
  }

  /**
    * Generates code for this table, which is not supported now.
    *
    * @param clazz The desired collection class; for example { @code Queryable}
    * @return
    */
  override def getExpression(clazz: java.lang.Class[_]): Expression =
    throw new UnsupportedOperationException

  /**
    * Obtains whether the ordinal column has a default value, which is not supported now.
    *
    * @param rowType            rowType of field
    * @param ordinal            indice of the given column
    * @param initializerContext the context for { @link InitializerExpressionFactory} methods
    * @return true if the column has a default value
    */
  override def columnHasDefaultValue(
      rowType: RelDataType,
      ordinal: Int,
      initializerContext: InitializerContext): Boolean = false

  /**
    * Gets flink table statistics.
    *
    * @return the flink table statistics.
    */
  def getFlinkStatistic: FlinkStatistic = {
    if (table.getStatistic != null) {
      table.getStatistic
    } else {
      FlinkStatistic.UNKNOWN
    }
  }

  override def getColumnStrategies: JList[ColumnStrategy] = RelOptTableImpl.columnStrategies(this)

  override def extend(extendedTable: Table) =
    throw new RuntimeException("Extending column not supported")

  override def config(qualifiedNames: JList[String], configuredTable: Table) =
    new FlinkRelOptTable(
      schema, getRowType, qualifiedNames, configuredTable.asInstanceOf[FlinkTable])
}

object FlinkRelOptTable {

  def create(schema: RelOptSchema,
      rowType: RelDataType,
      names: JList[String],
      table: FlinkTable): FlinkRelOptTable =
    new FlinkRelOptTable(schema, rowType, names, table)

}
