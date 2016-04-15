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

package org.apache.flink.api.table

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.table.JavaBatchTranslator
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.plan.TranslationContext
import org.apache.flink.api.table.plan.schema.{DataSetTable, TableTable}

class AbstractTableEnvironment {

  private[flink] val config = new TableConfig()

  /**
   * Returns the table config to define the runtime behavior of the Table API.
   */
  def getConfig = config

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
      throw new TableException(s"Table \'$tableName\' was not found in the registry.")
    }
  }

  private[flink] def registerDataSetInternal[T](name: String, dataset: DataSet[T]): Unit = {

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo[T](dataset.getType)
    val dataSetTable = new DataSetTable[T](
      dataset,
      fieldIndexes,
      fieldNames
    )
    TranslationContext.registerTable(dataSetTable, name)
  }

  private[flink] def registerDataSetInternal[T](
      name: String, dataset: DataSet[T], fields: Array[Expression]): Unit = {

    val (fieldNames, fieldIndexes) = TranslationContext.getFieldInfo[T](
      dataset.getType, fields.toArray)

    val dataSetTable = new DataSetTable[T](
      dataset,
      fieldIndexes.toArray,
      fieldNames.toArray
    )
    TranslationContext.registerTable(dataSetTable, name)
  }

  /**
   * Execute a SQL query and retrieve the result as a [[Table]].
   * All input [[Table]]s have to be registered in the
   * [[org.apache.flink.api.java.table.TableEnvironment]] with unique names,
   * using [[registerTable()]] or
   * [[org.apache.flink.api.java.table.TableEnvironment.registerDataSet()]]
   *
   * @param query the SQL query
   * @return the result of the SQL query as a [[Table]]
   */
  def sql(query: String): Table = {
    new JavaBatchTranslator(config).translateSQL(query)
  }
}
