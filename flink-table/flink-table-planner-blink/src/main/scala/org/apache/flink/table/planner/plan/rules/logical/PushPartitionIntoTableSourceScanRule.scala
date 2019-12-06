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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException
import org.apache.flink.table.catalog.{Catalog, CatalogPartitionSpec, ObjectIdentifier}
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, PartitionPruner, RexNodeExtractor}
import org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.sources.PartitionableTableSource

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rex.{RexInputRef, RexNode, RexShuttle}

import java.util

import scala.collection.JavaConversions._

/**
  * Planner rule that tries to push partitions evaluated by filter condition into a
  * [[PartitionableTableSource]].
  */
class PushPartitionIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[Filter],
    operand(classOf[LogicalTableScan], none)),
  "PushPartitionIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val filter: Filter = call.rel(0)
    if (filter.getCondition == null) {
      return false
    }

    val scan: LogicalTableScan = call.rel(1)
    scan.getTable.unwrap(classOf[TableSourceTable[_]]) match {
      case table: TableSourceTable[_] =>
        table.catalogTable.isPartitioned &&
          table.tableSource.isInstanceOf[PartitionableTableSource]
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val scan: LogicalTableScan = call.rel(1)
    val context = call.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val config = context.getTableConfig
    val tableSourceTable: TableSourceTable[_] = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val tableIdentifier = tableSourceTable.tableIdentifier
    val catalogOption = toScala(context.getCatalogManager.getCatalog(
      tableIdentifier.getCatalogName))

    val partitionFieldNames = tableSourceTable.catalogTable.getPartitionKeys.toSeq.toArray[String]

    val tableSource = tableSourceTable.tableSource.asInstanceOf[PartitionableTableSource]
    val inputFieldType = filter.getInput.getRowType
    val inputFields = inputFieldType.getFieldNames.toList.toArray

    val relBuilder = call.builder()
    val maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan)
    val (partitionPredicate, nonPartitionPredicate) =
      RexNodeExtractor.extractPartitionPredicates(
        filter.getCondition,
        maxCnfNodeCount,
        inputFields,
        relBuilder.getRexBuilder,
        partitionFieldNames
      )

    if (partitionPredicate.isAlwaysTrue) {
      // no partition predicates in filter
      return
    }

    val finalPartitionPredicate = adjustPartitionPredicate(
      inputFieldType.getFieldNames.toList.toArray,
      partitionFieldNames,
      partitionPredicate
    )
    val partitionFieldTypes = partitionFieldNames.map { name =>
      val index = inputFieldType.getFieldNames.indexOf(name)
      require(index >= 0, s"$name is not found in ${inputFieldType.getFieldNames.mkString(", ")}")
      inputFieldType.getFieldList.get(index).getType
    }.map(FlinkTypeFactory.toLogicalType)

    val allPartitions = tableSource.getPartitions
    val remainingPartitions = PartitionPruner.prunePartitions(
      config,
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      finalPartitionPredicate
    )

    val newTableSource = tableSource.applyPartitionPruning(remainingPartitions)

    if (newTableSource.explainSource().equals(tableSourceTable.tableSource.explainSource())) {
      throw new TableException("Failed to push partition into table source! "
        + "table source with pushdown capability must override and change "
        + "explainSource() API to explain the pushdown applied!")
    }

    val statistic = tableSourceTable.getStatistic
    val newStatistic = {
      val tableStats = catalogOption match {
        case Some(catalog) =>
          def mergePartitionStats(): TableStats = {
            var stats: TableStats = null
            for (p <- remainingPartitions) {
              getPartitionStats(catalog, tableIdentifier, p) match {
                case Some(currStats) =>
                  if (stats == null) {
                    stats = currStats
                  } else {
                    stats = stats.merge(currStats)
                  }
                case None => return null
              }
            }
            stats
          }
          mergePartitionStats()
        case None => null
      }
      FlinkStatistic.builder().statistic(statistic).tableStats(tableStats).build()
    }
    val newTableSourceTable = tableSourceTable.copy(newTableSource, newStatistic)

    val newScan = new LogicalTableScan(scan.getCluster, scan.getTraitSet, newTableSourceTable)
    // check whether framework still need to do a filter
    if (nonPartitionPredicate.isAlwaysTrue) {
      call.transformTo(newScan)
    } else {
      val newFilter = filter.copy(filter.getTraitSet, newScan, nonPartitionPredicate)
      call.transformTo(newFilter)
    }
  }

  private def getPartitionStats(
      catalog: Catalog,
      objectIdentifier: ObjectIdentifier,
      partSpec: util.Map[String, String]): Option[TableStats] = {
    val tablePath = objectIdentifier.toObjectPath
    val spec = new CatalogPartitionSpec(new util.LinkedHashMap[String, String](partSpec))
    try {
      val tableStatistics = catalog.getPartitionStatistics(tablePath, spec)
      val columnStatistics = catalog.getPartitionColumnStatistics(tablePath, spec)
      Some(CatalogTableStatisticsConverter.convertToTableStats(tableStatistics, columnStatistics))
    } catch {
      case _: PartitionNotExistException => None
    }
  }

  /**
    * adjust the partition field reference index to evaluate the partition values.
    * e.g. the original input fields is: a, b, c, p, and p is partition field. the partition values
    * are: List(Map("p"->"1"), Map("p" -> "2"), Map("p" -> "3")). If the original partition
    * predicate is $3 > 1. after adjusting, the new predicate is ($0 > 1).
    * and use ($0 > 1) to evaluate partition values (row(1), row(2), row(3)).
    */
  private def adjustPartitionPredicate(
      inputFieldNames: Array[String],
      partitionFieldNames: Array[String],
      partitionPredicate: RexNode): RexNode = {
    partitionPredicate.accept(new RexShuttle() {
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        val index = inputRef.getIndex
        val fieldName = inputFieldNames(index)
        val newIndex = partitionFieldNames.indexOf(fieldName)
        require(newIndex >= 0, s"$fieldName is not found in ${partitionFieldNames.mkString(", ")}")
        if (index == newIndex) {
          inputRef
        } else {
          new RexInputRef(newIndex, inputRef.getType)
        }
      }
    })
  }
}

object PushPartitionIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushPartitionIntoTableSourceScanRule
}
