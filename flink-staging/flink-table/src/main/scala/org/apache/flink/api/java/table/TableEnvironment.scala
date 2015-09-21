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
package org.apache.flink.api.java.table

import org.apache.flink.api.common.AbstractExecutionEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.Table
import org.apache.flink.api.table.input.HCatTableSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Environment for working with the Table API.
 *
 * This can be used to convert [[DataSet]] or [[DataStream]] to a [[Table]] and back again. You
 * can also use the provided methods to create a [[Table]] directly from a data source.
 */
class TableEnvironment(private[flink] val environment: AbstractExecutionEnvironment) {

  private def translatorFromEnv = {
    if (environment.isInstanceOf[ExecutionEnvironment]) {
      new JavaBatchTranslator(environment.asInstanceOf[ExecutionEnvironment])
    }
    else if (environment.isInstanceOf[StreamExecutionEnvironment]) {
      new JavaStreamingTranslator(environment.asInstanceOf[StreamExecutionEnvironment])
    }
    else {
      throw new IllegalArgumentException("ExecutionEnvironment is invalid for the " +
        "Java TableEnvironment.");
    }
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   tableEnv.fromDataSet(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def fromDataSet[T](set: DataSet[T], fields: String): Table = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaBatchTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataSets.")
    }
    translator.asInstanceOf[JavaBatchTranslator].createTable(set, fields)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataSet[T](set: DataSet[T]): Table = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaBatchTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataSets.")
    }
    translator.asInstanceOf[JavaBatchTranslator].createTable(set)
  }

  /**
   * Transforms the given DataStream to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataStream type are renamed to the given set of fields:
   *
   * Example:
   *
   * {{{
   *   tableEnv.fromDataStream(set, "a, b")
   * }}}
   *
   * This will transform the set containing elements of two fields to a table where the fields
   * are named a and b.
   */
  def fromDataStream[T](set: DataStream[T], fields: String): Table = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaStreamingTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataStreams.")
    }
    translator.asInstanceOf[JavaStreamingTranslator].createTable(set, fields)
  }

  /**
   * Transforms the given DataStream to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataStream type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataStream[T](set: DataStream[T]): Table = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaStreamingTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataStreams.")
    }
    translator.asInstanceOf[JavaStreamingTranslator].createTable(set)
  }

  /**
   * Reads a [[org.apache.flink.api.table.Table]] from a HCatalog data source.
   *
   * Make sure that the hive-site.xml is included in your class path.
   */
  def fromHCat[T](database: String, table: String): Table = {
    translatorFromEnv.createTable(new HCatTableSource(database, table))
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataSet. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.table.Table]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaBatchTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataSets.")
    }
    translator.asInstanceOf[JavaBatchTranslator].translate[T](table.operation)(
      TypeExtractor.createTypeInfo(clazz).asInstanceOf[TypeInformation[T]])
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataStream. The given type must have exactly the same fields as the
   * [[org.apache.flink.api.table.Table]]. That is, the names of the
   * fields and the types must match.
   */
  @SuppressWarnings(Array("unchecked"))
  def toDataStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    val translator = translatorFromEnv
    if (!translator.isInstanceOf[JavaStreamingTranslator]) {
      throw new IllegalArgumentException("ExecutionEnvironment does not support Java DataStreams.")
    }
    translator.asInstanceOf[JavaStreamingTranslator].translate[T](table.operation)(
      TypeExtractor.createTypeInfo(clazz).asInstanceOf[TypeInformation[T]])
  }
}

