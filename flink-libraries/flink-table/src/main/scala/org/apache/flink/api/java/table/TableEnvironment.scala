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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.expressions.ExpressionParser
import org.apache.flink.api.table.plan.TranslationContext
import org.apache.flink.api.table.plan.schema.{TableTable, DataSetTable}
import org.apache.flink.api.table.{TableException, TableConfig, Table}

/**
 * Environment for working with the Table API.
 *
 * This can be used to convert a [[DataSet]] to a [[Table]] and back again. You
 * can also use the provided methods to create a [[Table]] directly from a data source.
 */
class TableEnvironment {

  private val config = new TableConfig()

  /**
   * Returns the table config to define the runtime behavior of the Table API.
   */
  def getConfig = config

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
    new JavaBatchTranslator(config).createTable(set, fields)
  }

  /**
   * Transforms the given DataSet to a [[org.apache.flink.api.table.Table]].
   * The fields of the DataSet type are used to name the
   * [[org.apache.flink.api.table.Table]] fields.
   */
  def fromDataSet[T](set: DataSet[T]): Table = {
    new JavaBatchTranslator(config).createTable(set)
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataSet. The given type must have exactly the same field types and field order as the
   * [[org.apache.flink.api.table.Table]]. Row and tuple types can be mapped by position.
   * POJO types require name equivalence to be mapped correctly as their fields do not have
   * an order.
   */
  @SuppressWarnings(Array("unchecked"))
  def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = {
    new JavaBatchTranslator(config).translate[T](table.relNode)(
      TypeExtractor.createTypeInfo(clazz).asInstanceOf[TypeInformation[T]])
  }

  /**
   * Converts the given [[org.apache.flink.api.table.Table]] to
   * a DataSet. The given type must have exactly the same field types and field order as the
   * [[org.apache.flink.api.table.Table]]. Row and tuple types can be mapped by position.
   * POJO types require name equivalence to be mapped correctly as their fields do not have
   * an order.
   */
  def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = {
    new JavaBatchTranslator(config).translate[T](table.relNode)(typeInfo)
  }

  /**
   * Registers a DataSet under a unique name, so that it can be used in SQL queries.
   * The fields of the DataSet type are used to name the Table fields.
   * @param name the Table name
   * @param dataset the DataSet to register
   */
  def registerDataSet[T](name: String, dataset: DataSet[T]): Unit = {

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo[T](dataset.getType)
    val dataSetTable = new DataSetTable[T](
      dataset,
      fieldIndexes,
      fieldNames
    )
    TranslationContext.addAndRegisterDataSet(dataSetTable, name)
  }

  /**
   * Registers a DataSet under a unique name, so that it can be used in SQL queries.
   * The fields of the DataSet type are renamed to the given set of fields.
   *
   * @param name the Table name
   * @param dataset the DataSet to register
   * @param fields the Table field names
   */
  def registerDataSet[T](name: String, dataset: DataSet[T], fields: String): Unit = {

    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo[T](dataset.getType, exprs)

    val dataSetTable = new DataSetTable[T](
      dataset,
      fieldIndexes.toArray,
      fieldNames.toArray
    )
    TranslationContext.addAndRegisterDataSet(dataSetTable, name)
  }

  /**
   * Registers a Table under a unique name, so that it can be used in SQL queries.
   * @param name the Table name
   * @param table the Table to register
   */
  def registerTable[T](name: String, table: Table): Unit = {
    val tableTable = new TableTable(table.getRelNode())
    TranslationContext.registerTable(tableTable, name)
  }

  /**
   * Retrieve a registered Table.
   * @param tableName the name under which the Table has been registered
   * @return the Table object
   */
  @throws[TableException]
  def scan(tableName: String): Table = {
    if (TranslationContext.isRegistered(tableName)) {
      val relBuilder = TranslationContext.getRelBuilder
      relBuilder.scan(tableName)
      new Table(relBuilder.build(), relBuilder)
    }
    else {
      throw new TableException("Table \"" + tableName + "\" was not found in the registry.")
    }

  }
}
