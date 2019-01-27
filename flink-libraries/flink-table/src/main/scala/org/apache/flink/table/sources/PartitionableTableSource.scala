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
  */
abstract class PartitionableTableSource extends FilterableTableSource {

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
    * @return true only if the result is correct without partition predicate
    */
  def supportDropPartitionPredicate: Boolean = false

  /**
    * @return Pruned partitions
    */
  def getPrunedPartitions: JList[Partition]

  /**
    * @return True if apply partition pruning
    */
  def isPartitionPruned: Boolean

  /**
    * If a partitionable table source which can't apply non-partition filters should not pick any
    * predicates.
    * If a partitionable table source which can apply non-partition filters should check and pick
    * only predicates this table source can support.
    *
    * After trying to push pruned-partitions and predicates down, we should return a new
    * [[TableSource]] instance which holds all pruned-partitions and all pushed down predicates.
    * Even if we actually pushed nothing down, it is recommended that we still return a new
    * [[TableSource]] instance since we will mark the returned instance as filter push down has
    * been tried.
    * <p>
    * We also should note to not changing the form of the predicates passed in. It has been
    * organized in CNF conjunctive form, and we should only take or leave each element from the
    * list. Don't try to reorganize the predicates if you are absolutely confident with that.
    *
    * @param partitionPruned  Whether partition pruning is applied.
    * @param prunedPartitions Remaining partitions after partition pruning applied.
    *                         Notes: If partition pruning is not applied, prunedPartitions is empty.
    * @param predicates       A list contains conjunctive predicates, you should pick and remove all
    *                         expressions that can be pushed down. The remaining elements of this
    *                         list will further evaluated by framework.
    * @return A new cloned instance of [[TableSource]].
    */
  def applyPrunedPartitionsAndPredicate(
    partitionPruned: Boolean,
    prunedPartitions: JList[Partition],
    predicates: JList[Expression]): TableSource


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
  override def applyPredicate(predicates: JList[Expression]): TableSource = {
    var partitionPruned = false
    var prunedPartitions: JList[Partition] = new JArrayList()

    // extract partition predicate
    val (partitionPredicates, _) = PartitionPredicateExtractor.extractPartitionPredicates(
      predicates.asScala.toArray, getPartitionFieldNames)
    if (partitionPredicates.nonEmpty) {
      // do partition pruning
      val builder = relBuilder.getOrElse(throw new TableException("relBuilder is null"))
      prunedPartitions = applyPartitionPruning(partitionPredicates, builder)
      partitionPruned = true
    }

    if (supportDropPartitionPredicate) {
      predicates.removeAll(partitionPredicates.toList.asJava)
    }

    applyPrunedPartitionsAndPredicate(partitionPruned, prunedPartitions, predicates)
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
    * @return The pruned partitions.
    */
  def applyPartitionPruning(
    partitionPredicates: Array[Expression],
    relBuilder: RelBuilder): JList[Partition] = {
    PartitionPruner.INSTANCE.getPrunedPartitions(
      getPartitionFieldNames,
      getPartitionFieldTypes,
      getAllPartitions,
      partitionPredicates,
      relBuilder)
  }

}

/**
  * The base class of partition
  */
trait Partition {

  /**
    * Get partition field value by name
    *
    * @param fieldName Partition field name
    * @return Partition field value associated with the given key
    */
  def getFieldValue(fieldName: String): Any

  /**
    * @return Origin partition value
    */
  def getOriginValue: Any
}
