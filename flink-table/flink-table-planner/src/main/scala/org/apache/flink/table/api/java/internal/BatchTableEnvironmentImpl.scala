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
package org.apache.flink.table.api.java.internal

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.java.BatchTableEnvironment
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.module.ModuleManager

import _root_.scala.collection.JavaConverters._

/**
  * The implementation for the Java [[BatchTableEnvironment]] that works with [[DataSet]].
  *
  * @param execEnv The Java batch [[ExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  * @deprecated This constructor will be removed. Use [[BatchTableEnvironment#create()]] instead.
  */
class BatchTableEnvironmentImpl(
    execEnv: ExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager,
    moduleManager: ModuleManager)
  extends BatchTableEnvImpl(
    execEnv,
    config,
    catalogManager,
    moduleManager)
  with org.apache.flink.table.api.java.BatchTableEnvironment {

  override def fromDataSet[T](dataSet: DataSet[T]): Table = {
    createTable(asQueryOperation(dataSet, None))
  }

  override def fromDataSet[T](dataSet: DataSet[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields).asScala
      .toArray

    createTable(asQueryOperation(dataSet, Some(exprs)))
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = {
    registerTable(name, fromDataSet(dataSet))
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit = {
    registerTable(name, fromDataSet(dataSet, fields))
  }

  override def createTemporaryView[T](
      path: String,
      dataSet: DataSet[T]): Unit = {
    createTemporaryView(path, fromDataSet(dataSet))
  }

  override def createTemporaryView[T](
      path: String,
      dataSet: DataSet[T],
      fields: String): Unit = {
    createTemporaryView(path, fromDataSet(dataSet, fields))
  }

  override def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    // Use the default query config.
    translate[T](table)(TypeExtractor.createTypeInfo(clazz))
  }

  override def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = {
    // Use the default batch query config.
    translate[T](table)(typeInfo)
  }

  override def toDataSet[T](
      table: Table,
      clazz: Class[T],
      queryConfig: BatchQueryConfig): DataSet[T] = {
    translate[T](table)(TypeExtractor.createTypeInfo(clazz))
  }

  override def toDataSet[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: BatchQueryConfig): DataSet[T] = {
    translate[T](table)(typeInfo)
  }

  override def registerFunction[T](name: String, tf: TableFunction[T]): Unit = {
    implicit val typeInfo: TypeInformation[T] = TypeExtractor
      .createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0)
      .asInstanceOf[TypeInformation[T]]

    registerTableFunctionInternal[T](name, tf)
  }

  override def registerFunction[T, ACC](
      name: String,
      f: AggregateFunction[T, ACC])
  : Unit = {
    implicit val typeInfo: TypeInformation[T] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 0)
      .asInstanceOf[TypeInformation[T]]

    implicit val accTypeInfo: TypeInformation[ACC] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[T, ACC]], f.getClass, 1)
      .asInstanceOf[TypeInformation[ACC]]

    registerAggregateFunctionInternal[T, ACC](name, f)
  }

  override def sqlUpdate(
    stmt: String,
    config: BatchQueryConfig): Unit = sqlUpdate(stmt)

  override def insertInto(
    table: Table,
    queryConfig: BatchQueryConfig,
    sinkPath: String,
    sinkPathContinued: String*): Unit = insertInto(table, sinkPath, sinkPathContinued: _*)
}
