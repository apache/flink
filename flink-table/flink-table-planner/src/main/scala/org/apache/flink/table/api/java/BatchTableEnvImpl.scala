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
package org.apache.flink.table.api.java

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

import _root_.scala.collection.JavaConverters._

/**
  * The implementation for the Java [[BatchTableEnvironment]] that works with [[DataSet]].
  *
  * @param execEnv The Java batch [[ExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  * @deprecated This constructor will be removed. Use [[BatchTableEnvironment#create()]] instead.
  */
class BatchTableEnvImpl(
    execEnv: ExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.BatchTableEnvImpl(execEnv, config)
    with org.apache.flink.table.api.java.BatchTableEnvironment {

  override def fromDataSet[T](dataSet: DataSet[T]): Table = {

    val name = createUniqueTableName()
    registerDataSetInternal(name, dataSet)
    scan(name)
  }

  override def fromDataSet[T](dataSet: DataSet[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields).asScala
      .toArray

    val name = createUniqueTableName()
    registerDataSetInternal(name, dataSet, exprs)
    scan(name)
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = {

    checkValidTableName(name)
    registerDataSetInternal(name, dataSet)
  }

  override def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields).asScala
      .toArray

    checkValidTableName(name)
    registerDataSetInternal(name, dataSet, exprs)
  }

  override def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    // Use the default query config.
    translate[T](table, queryConfig)(TypeExtractor.createTypeInfo(clazz))
  }

  override def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = {
    // Use the default batch query config.
    translate[T](table, queryConfig)(typeInfo)
  }

  override def toDataSet[T](
      table: Table,
      clazz: Class[T],
      queryConfig: BatchQueryConfig): DataSet[T] = {
    translate[T](table, queryConfig)(TypeExtractor.createTypeInfo(clazz))
  }

  override def toDataSet[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: BatchQueryConfig): DataSet[T] = {
    translate[T](table, queryConfig)(typeInfo)
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
}
