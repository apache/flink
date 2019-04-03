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

import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{QueryConfig, Table, TableConfig, TableEnvironment}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

class MockTableEnvironment extends TableEnvironment(new TableConfig) {

  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = ???

  override protected def checkValidTableName(name: String): Unit = ???

  override def sqlQuery(query: String): Table = ???

  override protected def getBuiltInNormRuleSet: RuleSet = ???

  override protected def getBuiltInPhysicalOptRuleSet: RuleSet = ???

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit = ???

  override def registerTableSink(name: String, tableSink: TableSink[_]): Unit = ???

  override protected def createUniqueTableName(): String = ???

  override protected def registerTableSourceInternal(name: String, tableSource: TableSource[_])
    : Unit = ???

  override def explain(table: Table): String = ???

  override def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor = ???
}
