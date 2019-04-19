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

import org.apache.calcite.rex.RexLiteral
import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.sources.{DefinedIndexes, DefinedPrimaryKey, TableIndex, TableSource}

import scala.collection.JavaConverters._

/**
  * Utilities for temporal table join
  */
object LookupJoinUtil {

  /**
    * A [[LookupKey]] is a field used as equal condition when querying content from dimension table
    */
  sealed trait LookupKey

  /**
    * A [[LookupKey]] whose value is constant.
    * @param dataType the field type in TableSource
    * @param literal the literal value
    */
  case class ConstantLookupKey(dataType: InternalType, literal: RexLiteral) extends LookupKey

  /**
    * A [[LookupKey]] whose value comes from left table field.
    * @param index the index of the field in left table
    */
  case class FieldRefLookupKey(index: Int) extends LookupKey

  /**
    * Gets [[TableIndex]]s from a [[TableSource]]. This will combine primary key information
    * of [[DefinedPrimaryKey]] and indexes information of [[DefinedIndexes]].
    */
  def getTableIndexes(table: TableSource[_]): Array[TableIndex] = {
    val indexes: Array[TableIndex] = table match {
      case t: DefinedIndexes if t.getIndexes != null => t.getIndexes.asScala.toArray
      case _ => Array()
    }

    // add primary key into index list because primary key is an index too
    table match {
      case t: DefinedPrimaryKey =>
        val primaryKey = t.getPrimaryKeyColumns
        if (primaryKey != null && !primaryKey.isEmpty) {
          val primaryKeyIndex = TableIndex.builder()
              .uniqueIndex()
              .indexedColumns(primaryKey)
              .build()
          indexes ++ Array(primaryKeyIndex)
        } else {
          indexes
        }
      case _ => indexes
    }
  }

}
