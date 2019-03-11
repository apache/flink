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

package org.apache.flink.table.plan.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.physical.batch.BatchPhysicalRel
import org.apache.flink.table.typeutils.BinaryRowSerializer

import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelFieldCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Utility methods for sort operator.
  */
object SortUtil {

  /**
    * Returns the direction of the first sort field.
    *
    * @param collationSort The list of sort collations.
    * @return The direction of the first sort field.
    */
  def getFirstSortDirection(collationSort: RelCollation): Direction = {
    collationSort.getFieldCollations.get(0).direction
  }

  /**
    * Returns the first sort field.
    *
    * @param collationSort The list of sort collations.
    * @param rowType The row type of the input.
    * @return The first sort field.
    */
  def getFirstSortField(collationSort: RelCollation, rowType: RelDataType): RelDataTypeField = {
    val idx = collationSort.getFieldCollations.get(0).getFieldIndex
    rowType.getFieldList.get(idx)
  }

  /** Returns the default null direction if not specified. */
  def getNullDefaultOrders(ascendings: Array[Boolean]): Array[Boolean] = {
    ascendings.map { asc =>
      RelFieldCollationUtil.defaultNullCollation.last(!asc)
    }
  }

  def getKeysAndOrders(
      fieldCollations: Seq[RelFieldCollation]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val fieldMappingDirections = fieldCollations.map(c =>
      (c.getFieldIndex, directionToOrder(c.getDirection)))
    val keys = fieldMappingDirections.map(_._1)
    val orders = fieldMappingDirections.map(_._2 == Order.ASCENDING)
    val nullsIsLast = fieldCollations.map(_.nullDirection).map {
      case RelFieldCollation.NullDirection.LAST => true
      case RelFieldCollation.NullDirection.FIRST => false
      case RelFieldCollation.NullDirection.UNSPECIFIED =>
        throw new TableException(s"Do not support UNSPECIFIED for null order.")
    }.toArray

    deduplicateSortKeys(keys.toArray, orders.toArray, nullsIsLast)
  }

  def deduplicateSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val keySet = new mutable.HashSet[Int]
    val keyBuffer = new mutable.ArrayBuffer[Int]
    val orderBuffer = new mutable.ArrayBuffer[Boolean]
    val nullsIsLastBuffer = new mutable.ArrayBuffer[Boolean]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        keyBuffer += keys(i)
        orderBuffer += orders(i)
        nullsIsLastBuffer += nullsIsLast(i)
      }
    }
    (keyBuffer.toArray, orderBuffer.toArray, nullsIsLastBuffer.toArray)
  }

  def directionToOrder(direction: Direction): Order = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }

  def offsetToString(offset: RexNode): String = {
    val offsetValue = if (offset != null) {
      RexLiteral.intValue(offset)
    } else {
      0
    }
    s"$offsetValue"
  }

  def limitToString(limit: RexNode): String = {
    if (limit != null) {
      s"${RexLiteral.intValue(limit)}"
    } else {
      "unlimited"
    }
  }

  def fetchToString(limit: RexNode, offset: RexNode): String = {
    val limitEnd = getFetchLimitEnd(limit, offset)
    if (limitEnd == Long.MaxValue) {
      "unlimited"
    } else {
      s"$limitEnd"
    }
  }

  def sortFieldsToString(
      collations: RelCollation,
      rowRelDataType: RelDataType): String = {
    val fieldCollations = collations.getFieldCollations
      .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

    fieldCollations
      .map(col => s"${rowRelDataType.getFieldNames.get(col._1)} ${col._2.getShortName}")
      .mkString(", ")
  }

  def getFetchLimitEnd(fetch: RexNode, offset: RexNode): Long = {
    if (fetch != null) {
      RexLiteral.intValue(fetch) + getFetchLimitStart(offset)
    } else {
      Long.MaxValue
    }
  }

  def getFetchLimitStart(offset: RexNode): Long = {
    if (offset != null) {
      RexLiteral.intValue(offset)
    } else {
      0L
    }
  }

  def sortToString(
      rowRelDataType: RelDataType,
      sortCollation: RelCollation,
      sortOffset: RexNode,
      sortFetch: RexNode): String = {
    s"Sort(by: ($$sortFieldsToString(sortCollation, rowRelDataType))," +
      (if (sortOffset != null) {
        " offset: $offsetToString(sortOffset),"
      } else {
        ""
      }) +
      (if (sortFetch != null) {
        " fetch: $fetchToString(sortFetch, sortOffset))"
      } else {
        ""
      })
  }

  def sortExplainTerms(
      pw: RelWriter,
      rowRelDataType: RelDataType,
      sortCollation: RelCollation,
      sortOffset: RexNode,
      sortFetch: RexNode): RelWriter = {
    pw.item("orderBy", sortFieldsToString(sortCollation, rowRelDataType))
      .itemIf("offset", offsetToString(sortOffset), sortOffset != null)
      .itemIf("fetch", fetchToString(sortFetch, sortOffset), sortFetch != null)
  }

  def calcNeedMemoryForSort(mq: RelMetadataQuery, input: RelNode): Double = {
    //TODO It's hard to make sure that the normalized key's length is accurate in optimized stage.
    // use SortCodeGenerator.MAX_NORMALIZED_KEY_LEN instead of 16
    val normalizedKeyBytes = 16
    val rowCount = mq.getRowCount(input)
    val averageRowSize = BatchPhysicalRel.binaryRowAverageSize(input)
    val recordAreaInBytes = rowCount * (averageRowSize + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES)
    // TODO use BinaryIndexedSortable.OFFSET_LEN instead of 8
    val indexAreaInBytes = rowCount * (normalizedKeyBytes + 8)
    recordAreaInBytes + indexAreaInBytes
  }
}
