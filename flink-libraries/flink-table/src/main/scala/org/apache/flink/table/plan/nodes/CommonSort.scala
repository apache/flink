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

import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`._
import scala.collection.JavaConverters._
import org.apache.flink.api.common.operators.Order


/**
 * Trait represents a collection of sort methods to manipulate the parameters
 */

trait CommonSort {
  
  private[flink] def offsetToString(offset: RexNode): String = {
    val offsetToString = s"$offset"
    offsetToString
  }
  
  
  private[flink] def sortFieldsToString(
      collationSort: RelCollation, 
      rowRelDataType: RelDataType): String = {
    val fieldCollations = collationSort.getFieldCollations.asScala  
    .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

    val sortFieldsToString = fieldCollations
      .map(col => s"${
        rowRelDataType.getFieldNames.get(col._1)} ${col._2.getShortName}" ).mkString(", ")
    
    sortFieldsToString
  }
  
  private[flink] def directionToOrder(direction: Direction) = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }
  
  private[flink] def fetchToString(fetch: RexNode, offset: RexNode): String = {
    val limitEnd = getFetchLimitEnd(fetch, offset)
    val fetchToString = if (limitEnd == Long.MaxValue) {
      "unlimited"
    } else {
      s"$limitEnd"
    }
    fetchToString
  }
  
  private[flink] def getFetchLimitEnd (fetch: RexNode, offset: RexNode): Long = {
    val limitEnd: Long = if (fetch != null) {
      RexLiteral.intValue(fetch) + getFetchLimitStart(offset)
    } else {
      Long.MaxValue
    }
    limitEnd
  }
  
  private[flink] def getFetchLimitStart (offset: RexNode): Long = {
     val limitStart: Long = if (offset != null) {
      RexLiteral.intValue(offset)
     } else {
       0L
     }
     limitStart
  }
  
}
