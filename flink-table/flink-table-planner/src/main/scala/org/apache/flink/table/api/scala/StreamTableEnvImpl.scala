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
package org.apache.flink.table.api.scala

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{StreamQueryConfig, Table, TableConfig, TableEnvImpl}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.asScalaStream

/**
  * The implementation for a Scala [[StreamTableEnvironment]].
  *
  * @param execEnv The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class StreamTableEnvImpl(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.StreamTableEnvImpl(
    execEnv.getWrappedStreamExecutionEnvironment,
    config)
    with org.apache.flink.table.api.scala.StreamTableEnvironment {

  override def fromDataStream[T](dataStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream)
    scan(name)
  }

  override def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream.javaStream, fields.toArray)
    scan(name)
  }

  override def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream)
  }

  override def registerDataStream[T](
    name: String, dataStream: DataStream[T], fields: Expression*): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream.javaStream, fields.toArray)
  }

  override def toAppendStream[T: TypeInformation](table: Table): DataStream[T] = {
    toAppendStream(table, queryConfig)
  }

  override def toAppendStream[T: TypeInformation](
    table: Table,
    queryConfig: StreamQueryConfig): DataStream[T] = {
    val returnType = createTypeInformation[T]
    asScalaStream(translate(
      table, queryConfig, updatesAsRetraction = false, withChangeFlag = false)(returnType))
  }

  override def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    toRetractStream(table, queryConfig)
  }

  override def toRetractStream[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig): DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]
    asScalaStream(
      translate(table, queryConfig, updatesAsRetraction = true, withChangeFlag = true)(returnType))
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
}
