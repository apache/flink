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

package org.apache.flink.table.utils

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.descriptors.{ConnectTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import java.util.Optional

import org.apache.flink.table.module.Module

class MockTableEnvironment extends TableEnvironment {

  override def fromTableSource(source: TableSource[_]): Table = ???

  override def registerFunction(name: String, function: ScalarFunction): Unit = ???

  override def registerTable(name: String, table: Table): Unit = ???

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = ???

  override def registerTableSink(
    name: String,
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]], tableSink: TableSink[_]): Unit = ???

  override def registerTableSink(name: String, configuredSink: TableSink[_]): Unit = ???

  override def scan(tablePath: String*): Table = ???

  override def connect(connectorDescriptor: ConnectorDescriptor): ConnectTableDescriptor = ???

  override def listCatalogs(): Array[String] = ???

  override def listModules(): Array[String] = ???

  override def listDatabases(): Array[String] = ???

  override def listTables(): Array[String] = ???

  override def listUserDefinedFunctions(): Array[String] = ???

  override def listFunctions(): Array[String] = ???

  override def explain(table: Table): String = ???

  override def explain(table: Table, extended: Boolean): String = ???

  override def explain(extended: Boolean): String = ???

  override def getCompletionHints(statement: String, position: Int): Array[String] = ???

  override def sqlQuery(query: String): Table = ???

  override def sqlUpdate(stmt: String): Unit = ???

  override def getConfig: TableConfig = ???

  override def registerCatalog(
    name: String,
    catalog: Catalog): Unit = ???

  override def getCatalog(catalogName: String): Optional[Catalog] = ???

  override def getCurrentCatalog: String = ???

  override def getCurrentDatabase: String = ???

  override def useCatalog(catalogName: String): Unit = ???

  override def useDatabase(databaseName: String): Unit = ???

  override def insertInto(sinkPath: String, table: Table): Unit = ???

  override def insertInto(
    table: Table,
    sinkPath: String,
    sinkPathContinued: String*): Unit = ???

  override def execute(jobName: String): JobExecutionResult = ???

  override def loadModule(moduleName: String, module: Module): Unit = ???

  override def unloadModule(moduleName: String): Unit = ???

  override def createTemporaryView(
    path: String,
    view: Table): Unit = ???

  override def listTemporaryTables(): Array[String] = ???

  override def listTemporaryViews(): Array[String] = ???

  override def from(path: String): Table = ???

  override def dropTemporaryTable(path: String): Boolean = ???

  override def dropTemporaryView(path: String): Boolean = ???

}
