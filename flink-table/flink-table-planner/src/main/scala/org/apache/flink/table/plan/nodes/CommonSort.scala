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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.{RelCollation, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.table.runtime.aggregate.SortUtil.directionToOrder

import scala.collection.JavaConverters._

/**
 * Common methods for Flink sort operators.
 */
trait CommonSort {
  
  private def offsetToString(offset: RexNode): String = {
    val offsetToString = s"$offset"
    offsetToString
  }
  
  private def sortFieldsToString(
      collationSort: RelCollation, 
      rowRelDataType: RelDataType): String = {
    val fieldCollations = collationSort.getFieldCollations.asScala  
      .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

    fieldCollations
      .map(col => s"${rowRelDataType.getFieldNames.get(col._1)} ${col._2.getShortName}" )
      .mkString(", ")
  }
  
  private def fetchToString(fetch: RexNode, offset: RexNode): String = {
    val limitEnd = getFetchLimitEnd(fetch, offset)
    
    if (limitEnd == Long.MaxValue) {
      "unlimited"
    } else {
      s"$limitEnd"
    }
  }
  
  private[flink] def getFetchLimitEnd (fetch: RexNode, offset: RexNode): Long = {
    if (fetch != null) {
      RexLiteral.intValue(fetch) + getFetchLimitStart(offset)
    } else {
      Long.MaxValue
    }
  }
  
  private[flink] def getFetchLimitStart (offset: RexNode): Long = {
    if (offset != null) {
      RexLiteral.intValue(offset)
    } else {
      0L
    }
  }
  
  private[flink] def sortToString(
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
  
  private[flink] def sortExplainTerms(
      pw: RelWriter,
      rowRelDataType: RelDataType,
      sortCollation: RelCollation,
      sortOffset: RexNode,
      sortFetch: RexNode) : RelWriter = {
    
    pw
      .item("orderBy", sortFieldsToString(sortCollation, rowRelDataType))
      .itemIf("offset", offsetToString(sortOffset), sortOffset != null)
      .itemIf("fetch", fetchToString(sortFetch, sortOffset), sortFetch != null)
  }
  
}
