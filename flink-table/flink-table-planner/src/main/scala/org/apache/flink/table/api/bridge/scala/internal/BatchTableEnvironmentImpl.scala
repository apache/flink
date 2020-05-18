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
package org.apache.flink.table.api.bridge.scala.internal

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.util.DummyExecutionEnvironment

import _root_.scala.reflect.ClassTag

/**
  * The implementation for a Scala [[BatchTableEnvironment]] that works
  * with [[DataSet]]s.
  *
  * @param execEnv The Scala batch [[ExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class BatchTableEnvironmentImpl(
    execEnv: ExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager,
    moduleManager: ModuleManager)
  extends BatchTableEnvImpl(
    execEnv.getJavaEnv,
    config,
    catalogManager,
    moduleManager)
  with org.apache.flink.table.api.bridge.scala.BatchTableEnvironment {

  override def fromDataSet[T](dataSet: DataSet[T]): Table = {
    createTable(asQueryOperation(dataSet.javaSet, None))
  }

  override def fromDataSet[T](dataSet: DataSet[T], fields: Expression*): Table = {
    createTable(asQueryOperation(dataSet.javaSet, Some(fields.toArray)))
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = {
    registerTable(name, fromDataSet(dataSet))
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T], fields: Expression*): Unit = {
    registerTable(name, fromDataSet(dataSet, fields: _*))
  }

  override def toDataSet[T: TypeInformation](table: Table): DataSet[T] = {
    // Use the default batch query config.
    wrap[T](translate(table))(ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
  }

  override def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    registerTableFunctionInternal(name, tf)
  }

  override def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC])
  : Unit = {
    registerAggregateFunctionInternal[T, ACC](name, f)
  }

  override def createTemporaryView[T](
      path: String,
      dataSet: DataSet[T]): Unit = {
    createTemporaryView(path, fromDataSet(dataSet))
  }

  override def createTemporaryView[T](
      path: String,
      dataSet: DataSet[T],
      fields: Expression*): Unit = {
    createTemporaryView(path, fromDataSet(dataSet, fields: _*))
  }

  override protected def createDummyBatchTableEnv(): BatchTableEnvImpl = {
    new BatchTableEnvironmentImpl(
      new ExecutionEnvironment(new DummyExecutionEnvironment(execEnv.getJavaEnv)),
      config,
      catalogManager,
      moduleManager
    )
  }
}

