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

package org.apache.flink.table.util

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.plan.cost.{FlinkCostFactory, FlinkStreamCost}
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

import _root_.scala.collection.mutable

class MockTableEnvironment extends TableEnvironment(
  StreamExecutionEnvironment.getExecutionEnvironment,
  new TableConfig) {

  val tableSources = new mutable.HashMap[String, TableSource]()

  def getTableSource(name: String): TableSource = {
    tableSources.get(name).orNull
  }

  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = ???

  override protected def getFlinkCostFactory: FlinkCostFactory = FlinkStreamCost.FACTORY

  override protected def checkValidTableName(name: String): Unit = ???

  override def sqlQuery(query: String): Table = ???

  override def registerTableSource(name: String, tableSource: TableSource): Unit = {
    tableSources += name -> tableSource
  }

  override def execute(): JobExecutionResult = ???

  override def execute(jobName: String): JobExecutionResult = ???

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_]): Unit = ???

  override def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = ???

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  override def explain(table: Table): String = ???

  override def explain(extended: Boolean): String = ???

  override def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor = ???

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param replace     Whether to replace this [[TableSource]]
    */
  override protected def registerTableSourceInternal(name: String,
      tableSource: TableSource,
      tableStats: FlinkStatistic,
      replace: Boolean): Unit = ???

  /**
    * Registers or replace an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name       The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink  The [[TableSink]] to register.
    * @param replace    Whether replace this [[TableSink]].
    */
  override protected def registerTableSinkInternal(name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_],
      replace: Boolean): Unit = ???

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name           The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  override protected def registerTableSinkInternal(name: String,
      configuredSink: TableSink[_],
      replace: Boolean): Unit = ???

  /**
    * Optimize the RelNode tree (or DAG), and translate the result to [[ExecNode]] tree (or DAG).
    */
  override private[flink] def optimizeAndTranslateNodeDag(
      dagOptimizeEnabled: Boolean,
      logicalNodes: LogicalNode*) = ???

  /**
    * Translates a [[ExecNode]] DAG into a [[StreamTransformation]] DAG.
    *
    * @param sinks The node DAG to translate.
    * @return The [[StreamTransformation]] DAG that corresponds to the node DAG.
    */
  override protected def translate(
      sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]] = ???
}
