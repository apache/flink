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
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.{AggregateFunction, TableFunction, TableAggregateFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import _root_.java.lang.{Boolean => JBool}
import _root_.scala.collection.JavaConverters._

/**
  * The implementation for a Java [[StreamTableEnvironment]].
  *
  * @param execEnv The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  * @deprecated This constructor will be removed. Use StreamTableEnvironment.create() instead.
  */
class StreamTableEnvImpl(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.StreamTableEnvImpl(execEnv, config)
    with org.apache.flink.table.api.java.StreamTableEnvironment {

  override def fromDataStream[T](dataStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream)
    scan(name)
  }

  override def fromDataStream[T](dataStream: DataStream[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields).asScala
      .toArray

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream, exprs)
    scan(name)
  }

  override def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream)
  }

  override def registerDataStream[T](
    name: String, dataStream: DataStream[T], fields: String): Unit = {

    val exprs = ExpressionParser
      .parseExpressionList(fields).asScala
      .toArray

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream, exprs)
  }

  override def toAppendStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    toAppendStream(table, clazz, queryConfig)
  }

  override def toAppendStream[T](table: Table, typeInfo: TypeInformation[T]): DataStream[T] = {
    toAppendStream(table, typeInfo, queryConfig)
  }

  override def toAppendStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[T] = {
    val typeInfo = TypeExtractor.createTypeInfo(clazz)
    TableEnvImpl.validateType(typeInfo)
    translate[T](table, queryConfig, updatesAsRetraction = false, withChangeFlag = false)(typeInfo)
  }

  override def toAppendStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[T] = {
    TableEnvImpl.validateType(typeInfo)
    translate[T](table, queryConfig, updatesAsRetraction = false, withChangeFlag = false)(typeInfo)
  }

  override def toRetractStream[T](
      table: Table,
      clazz: Class[T]): DataStream[JTuple2[JBool, T]] = {

    toRetractStream(table, clazz, queryConfig)
  }

  override def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T]): DataStream[JTuple2[JBool, T]] = {

    toRetractStream(table, typeInfo, queryConfig)
  }

  override def toRetractStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = {

    val typeInfo = TypeExtractor.createTypeInfo(clazz)
    TableEnvImpl.validateType(typeInfo)
    val resultType = new TupleTypeInfo[JTuple2[JBool, T]](Types.BOOLEAN, typeInfo)
    translate[JTuple2[JBool, T]](
      table,
      queryConfig,
      updatesAsRetraction = true,
      withChangeFlag = true)(resultType)
  }

  override def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = {

    TableEnvImpl.validateType(typeInfo)
    val resultTypeInfo = new TupleTypeInfo[JTuple2[JBool, T]](
      Types.BOOLEAN,
      typeInfo
    )
    translate[JTuple2[JBool, T]](
      table,
      queryConfig,
      updatesAsRetraction = true,
      withChangeFlag = true)(resultTypeInfo)
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
    registerUserDefinedAggregateFunction(name, f)
  }

  override def registerFunction[T, ACC](
    name: String,
    f: TableAggregateFunction[T, ACC])
  : Unit = {
    registerUserDefinedAggregateFunction(name, f)
  }

  /**
    * Common function for registering an [[AggregateFunction]] or a [[TableAggregateFunction]].
    */
  private def registerUserDefinedAggregateFunction[T, ACC](
    name: String,
    f: UserDefinedAggregateFunction[T, ACC])
  : Unit = {
    implicit val typeInfo: TypeInformation[T] = TypeExtractor
      .createTypeInfo(f, classOf[UserDefinedAggregateFunction[T, ACC]], f.getClass, 0)
      .asInstanceOf[TypeInformation[T]]

    implicit val accTypeInfo: TypeInformation[ACC] = TypeExtractor
      .createTypeInfo(f, classOf[UserDefinedAggregateFunction[T, ACC]], f.getClass, 1)
      .asInstanceOf[TypeInformation[ACC]]

    registerAggregateFunctionInternal[T, ACC](name, f)
  }
}
