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

package org.apache.flink.table.sources

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.util.{PartitionPredicateExtractor, PartitionPruner}

import scala.collection.JavaConverters._

/**
  * A [[TableSource]] extending this class is a partition table,
  * and will get the relevant partitions about the query.
  *
  * Knows more detail about partition pruning, please see [[PartitionPruner]].
  *
  * @tparam T The return type of the [[TableSource]].
  */
abstract class PartitionableTableSource[T] extends FilterableTableSource[T] {

  private var relBuilder: Option[RelBuilder] = None

  /**
    * Get all partitions belong to this table
    *
    * @return All partitions belong to this table
    */
  def getAllPartitions: JList[Partition]

  /**
    * Get partition field names.
    *
    * @return Partition field names.
    */
  def getPartitionFieldNames: Array[String]

  /**
    * Get partition field types.
    *
    * @return Partition field types.
    */
  def getPartitionFieldTypes: Array[TypeInformation[_]]

  /**
    * Whether drop partition predicates after apply partition pruning.
    *
    * @return True only if the filter result is correct without dropped partition predicates.
    */
  def supportDropPartitionPredicates: Boolean = false

  /**
    * @return The remaining partitions after partition pruning applied.
    */
  def getRemainingPartitions: JList[Partition]

  /**
    * @return True If the filter dose contain partition predicates, otherwise false.
    */
  def isPartitionPrunedApplied: Boolean

  /**
    * Apply partition pruning results.
    *
    * If a partitionable table source which can't apply non-partition filters should not pick any
    * predicates.
    * If a partitionable table source which can apply non-partition filters should check and pick
    * only predicates this table source can support.
    *
    * After trying to push remaining partitions and predicates down, we should return a new
    * [[TableSource]] instance which holds all remaining partitions and all pushed down predicates.
    * Even if we actually pushed nothing down, it is recommended that we still return a new
    * [[TableSource]] instance since we will mark the returned instance as filter push down has
    * been tried.
    * <p>
    * We also should note to not changing the form of the predicates passed in. It has been
    * organized in CNF conjunctive form, and we should only take or leave each element from the
    * list. Don't try to reorganize the predicates if you are absolutely confident with that.
    *
    * @param isPartitionPrunedApplied  Whether partition pruning is applied. If the filter dose
    *                                  contain partition predicates, isPartitionPrunedApplied is
    *                                  true; otherwise it's false.
    * @param remainingPartitions Remaining partitions after partition pruning applied. Notes: If
    *                            partition pruning is not applied, remainingPartitions is empty.
    * @param predicates       A list contains conjunctive predicates, you should pick and remove all
    *                         expressions that can be pushed down. The remaining elements of this
    *                         list will further evaluated by framework.
    * @return A new cloned instance of [[TableSource]].
    */
  def applyRemainingPartitionsAndPredicates(
    isPartitionPrunedApplied: Boolean,
    remainingPartitions: JList[Partition],
    predicates: JList[Expression]): TableSource[T]


  /**
    * Check and pick all predicates this table source can support. The passed in predicates
    * have been translated in conjunctive form, and table source can only pick those predicates
    * that it supports.
    * <p>
    * After trying to push predicates down, we should return a new [[TableSource]]
    * instance which holds all pushed down predicates. Even if we actually pushed nothing down,
    * it is recommended that we still return a new [[TableSource]] instance since we will
    * mark the returned instance as filter push down has been tried.
    * <p>
    * We also should note to not changing the form of the predicates passed in. It has been
    * organized in CNF conjunctive form, and we should only take or leave each element from the
    * list. Don't try to reorganize the predicates if you are absolutely confident with that.
    *
    * @param predicates A list contains conjunctive predicates, you should pick and remove all
    *                   expressions that can be pushed down. The remaining elements of this list
    *                   will further evaluated by framework.
    * @return A new cloned instance of [[TableSource]].
    */
  override def applyPredicate(predicates: JList[Expression]): TableSource[T] = {
    var hasPartitionPredicates = false
    var remainingPartitions: JList[Partition] = new JArrayList()

    // extract partition predicate
    val (partitionPredicates, _) = PartitionPredicateExtractor.extractPartitionPredicates(
      predicates.asScala.toArray, getPartitionFieldNames)
    if (partitionPredicates.nonEmpty) {
      // do partition pruning
      val builder = relBuilder.getOrElse(throw new TableException("relBuilder is null"))
      remainingPartitions = applyPartitionPruning(partitionPredicates, builder)
      hasPartitionPredicates = true
    }

    if (supportDropPartitionPredicates) {
      predicates.removeAll(partitionPredicates.toList.asJava)
    }

    applyRemainingPartitionsAndPredicates(hasPartitionPredicates, remainingPartitions, predicates)
  }

  /**
    * @param relBuilder Builder for relational expressions.
    */
  def setRelBuilder(relBuilder: RelBuilder): Unit = {
    this.relBuilder = Some(relBuilder)
  }

  /**
    * Default implementation for partition pruning.
    *
    * @param partitionPredicates A filter expression that will be applied against partition values.
    * @param relBuilder          Builder for relational expressions.
    * @return The remaining partitions after partition pruning applied.
    */
  def applyPartitionPruning(
    partitionPredicates: Array[Expression],
    relBuilder: RelBuilder): JList[Partition] = {
    PartitionPruner.INSTANCE.getRemainingPartitions(
      getPartitionFieldNames,
      getPartitionFieldTypes,
      getAllPartitions,
      partitionPredicates,
      relBuilder)
  }

}

/**
  * A Partition is a division of a logical table horizontally(row-wise) split by partition columns,
  * and data within a table is split across multiple partitions, each partition corresponds to a
  * particular rows.
  *
  * A Partition could be simple, and also could be composite consisting of other partitions.
  * For example: there is a logs table stored on file system,
  * A simple partition represents a file split by year. The path could be /logs/2015.csv,
  * /logs/2016.csv, so a simple partition could be "year=2015" or "year=2016".
  * A composite partition corresponds to a directory split by year, month and day. The path cloud be
  * /logs/2015/01/01.csv, /logs/2015/01/10.csv, /logs/2016/05/01.csv, /logs/2016/06/01.csv,
  * so a complex partition could be "year=2015,month=01,day=01" or "year=2016,month=06,day=01".
  */
trait Partition {

  /**
    * Get partition field value associated with the given name
    *
    * In the example above,
    * for partition "year=2015", "2015" is the expected value corresponding to field name "year".
    * for partition "year=2016,month=06,day=01", "2016" is the expected value corresponding to
    * field name "year", "06" is corresponding to "month", "01" is corresponding to "day".
    *
    * @param fieldName Partition field name.
    * @return Partition field value associated with the given name, if not found return null.
    */
  def getFieldValue(fieldName: String): Any

  /**
    * Gets the origin value which represents the entire partition value.
    *
    * In the example above, for simple partition "year=2015" is the origin partition value,
    * for complex partition "year=2016,month=06,day=01" is the origin partition value.
    *
    * @return Origin partition value
    */
  def getOriginValue: Any
}
