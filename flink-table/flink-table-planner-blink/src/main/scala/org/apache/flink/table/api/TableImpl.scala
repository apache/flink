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

package org.apache.flink.table.api

import org.apache.flink.table.`type`.TypeConverters.createExternalTypeInfoFromInternalType
import org.apache.flink.table.calcite.FlinkTypeFactory._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.operations.TableOperation

import _root_.scala.collection.JavaConversions._

/**
  * The implementation of the [[Table]].
  *
  * NOTE: Currently, [[TableImpl]] is just a wrapper for RelNode
  * and all the methods in the class are not implemented. This is
  * used to support end-to-end tests for Blink planner. It will be
  * implemented when we support full stack Table API for Blink planner.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param relNode  The Calcite RelNode representation
  */
class TableImpl(val tableEnv: TableEnvironment, relNode: RelNode) extends Table {

  private lazy val tableSchema: TableSchema = {
    val rowType = relNode.getRowType
    val fieldNames = rowType.getFieldList.map(_.getName)
    val fieldTypes = rowType.getFieldList map { tp =>
      val internalType = toInternalType(tp.getType)
      createExternalTypeInfoFromInternalType(internalType)
    }
    new TableSchema(fieldNames.toArray, fieldTypes.toArray)
  }

  /**
    * Returns the Calcite RelNode represent this Table.
    */
  def getRelNode: RelNode = relNode

  override def getSchema: TableSchema = tableSchema

  override def printSchema(): Unit = ???

  override def select(fields: String): Table = ???

  override def select(fields: Expression*): Table = ???

  override def createTemporalTableFunction(
    timeAttribute: String,
    primaryKey: String): TemporalTableFunction = ???

  override def createTemporalTableFunction(
    timeAttribute: Expression,
    primaryKey: Expression): TemporalTableFunction = ???

  override def as(fields: String): Table = ???

  override def as(fields: Expression*): Table = ???

  override def filter(predicate: String): Table = ???

  override def filter(predicate: Expression): Table = ???

  override def where(predicate: String): Table = ???

  override def where(predicate: Expression): Table = ???

  override def groupBy(fields: String): GroupedTable = ???

  override def groupBy(fields: Expression*): GroupedTable = ???

  override def distinct(): Table = ???

  override def join(right: Table): Table = ???

  override def join(
    right: Table,
    joinPredicate: String): Table = ???

  override def join(
    right: Table,
    joinPredicate: Expression): Table = ???

  override def leftOuterJoin(right: Table): Table = ???

  override def leftOuterJoin(
    right: Table,
    joinPredicate: String): Table = ???

  override def leftOuterJoin(
    right: Table,
    joinPredicate: Expression): Table = ???

  override def rightOuterJoin(
    right: Table,
    joinPredicate: String): Table = ???

  override def rightOuterJoin(
    right: Table,
    joinPredicate: Expression): Table = ???

  override def fullOuterJoin(
    right: Table,
    joinPredicate: String): Table = ???

  override def fullOuterJoin(
    right: Table,
    joinPredicate: Expression): Table = ???

  override def joinLateral(tableFunctionCall: String): Table = ???

  override def joinLateral(tableFunctionCall: Expression): Table = ???

  override def joinLateral(
    tableFunctionCall: String,
    joinPredicate: String): Table = ???

  override def joinLateral(
    tableFunctionCall: Expression,
    joinPredicate: Expression): Table = ???

  override def leftOuterJoinLateral(tableFunctionCall: String): Table = ???

  override def leftOuterJoinLateral(tableFunctionCall: Expression): Table = ???

  override def leftOuterJoinLateral(
    tableFunctionCall: String,
    joinPredicate: String): Table = ???

  override def leftOuterJoinLateral(
    tableFunctionCall: Expression,
    joinPredicate: Expression): Table = ???

  override def minus(right: Table): Table = ???

  override def minusAll(right: Table): Table = ???

  override def union(right: Table): Table = ???

  override def unionAll(right: Table): Table = ???

  override def intersect(right: Table): Table = ???

  override def intersectAll(right: Table): Table = ???

  override def orderBy(fields: String): Table = ???

  override def orderBy(fields: Expression*): Table = ???

  override def offset(offset: Int): Table = ???

  override def fetch(fetch: Int): Table = ???

  override def insertInto(tableName: String): Unit = ???

  override def insertInto(
    tableName: String,
    conf: QueryConfig): Unit = ???

  override def window(groupWindow: GroupWindow): GroupWindowedTable = ???

  override def window(overWindows: OverWindow*): OverWindowedTable = ???

  override def addColumns(fields: String): Table = ???

  override def addColumns(fields: Expression*): Table = ???

  override def addOrReplaceColumns(fields: String): Table = ???

  override def addOrReplaceColumns(fields: Expression*): Table = ???

  override def renameColumns(fields: String): Table = ???

  override def renameColumns(fields: Expression*): Table = ???

  override def dropColumns(fields: String): Table = ???

  override def dropColumns(fields: Expression*): Table = ???

  override def map(mapFunction: String): Table = ???

  override def map(mapFunction: Expression): Table = ???

  override def flatMap(tableFunction: String): Table = ???

  override def flatMap(tableFunction: Expression): Table = ???

  override def getTableOperation: TableOperation = ???

  override def aggregate(aggregateFunction: String): AggregatedTable = ???

  override def aggregate(aggregateFunction: Expression): AggregatedTable = ???

  override def flatAggregate(tableAggregateFunction: String): FlatAggregateTable = ???

  override def flatAggregate(tableAggregateFunction: Expression): FlatAggregateTable = ???
}
