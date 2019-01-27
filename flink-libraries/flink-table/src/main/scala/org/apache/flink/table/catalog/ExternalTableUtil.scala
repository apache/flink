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

package org.apache.flink.table.catalog

import java.util

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.ConnectorDescriptor
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory, StreamTableSinkFactory, StreamTableSourceFactory, TableFactoryService, TableSourceParserFactory}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.{Logging, TableProperties}
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConversions._

/**
  * The utility class is used to convert CatalogTable to TableSinkTable.
  */
object ExternalTableUtil extends Logging {

  private def convertTableSchemaToRichTableSchema(
      tableSchema: TableSchema): RichTableSchema = {

    val colNames = tableSchema.getFieldNames
    val colTypes = tableSchema.getFieldTypes
    val colNullables = tableSchema.getFieldNullables
    val richTableSchema = new RichTableSchema(
      colNames, colTypes, colNullables)
    val primaryKeys = tableSchema.getPrimaryKeys
    richTableSchema.setPrimaryKey(primaryKeys: _*)
    // TODO unique keys of RichTableSchema
    // TODO indexes of RichTableSchema
    // TODO header fields of RichTableSchema
    richTableSchema
  }

  /**
   * Converts table source parser from the given CatalogTable.
   *
   * @param name        the name of the table
   * @param table       the [[CatalogTable]] instance which to convert
   * @param isStreaming Is in streaming mode or not
   * @return the extracted parser
   */
  def toParser(
                name: String, table: CatalogTable, isStreaming: Boolean): TableSourceParser = {

    val tableProperties = generateTableProperties(name, table, isStreaming)
    try {
      val tableFactory = TableFactoryService.find(classOf[TableSourceParserFactory],
        getToolDescriptor(getStorageType(name, tableProperties), tableProperties))
      tableFactory.createParser(
        name, table.getRichTableSchema, tableProperties)
    } catch {
      // No TableSourceParserFactory found
      case e@ (_: AmbiguousTableFactoryException | _: NoMatchingTableFactoryException) => null
    }
  }

  /**
    * Converts an [[CatalogTable]] instance to a [[TableSource]] instance
    *
    * @param name the name of the table source
    * @param catalogTable the [[CatalogTable]] instance which to convert
    * @param isStreaming is streaming source expected.
    * @return converted [[TableSource]] instance from the input catalog table
    */
  def toTableSource(
     name: String,
     catalogTable: CatalogTable,
     isStreaming: Boolean): TableSource = {

        val tableProperties = generateTableProperties(name, catalogTable, isStreaming)
        if (isStreaming) {
          val tableFactory = TableFactoryService.find(
            classOf[StreamTableSourceFactory[_]],
            getToolDescriptor(getStorageType(name, tableProperties), tableProperties))
          tableFactory.createStreamTableSource(tableProperties.toKeyLowerCase.toMap)
        } else {
          val tableFactory = TableFactoryService.find(
            classOf[BatchTableSourceFactory[_]],
            getToolDescriptor(getStorageType(name, tableProperties), tableProperties))
          tableFactory.createBatchTableSource(tableProperties.toKeyLowerCase.toMap)
        }
  }

  /**
    * Converts an [[CatalogTable]] instance to a [[TableSink]] instance
    *
    * @param name          name of the table
    * @param externalTable the [[CatalogTable]] instance to convert
    * @param isStreaming   is in streaming mode or not.
    * @return
    */
  def toTableSink(
                   name: String,
                   externalTable: CatalogTable,
                   isStreaming: Boolean): TableSink[_] = {

    val tableProperties: TableProperties = generateTableProperties(name, externalTable, isStreaming)
    if (isStreaming) {
      val tableFactory = TableFactoryService.find(classOf[StreamTableSinkFactory[_]],
        getToolDescriptor(getStorageType(name, tableProperties), tableProperties))
      tableFactory.createStreamTableSink(tableProperties.toKeyLowerCase.toMap)
    } else {
      val tableFactory = TableFactoryService.find(classOf[BatchTableSinkFactory[_]],
        getToolDescriptor(getStorageType(name, tableProperties), tableProperties))
      tableFactory.createBatchTableSink(tableProperties.toKeyLowerCase.toMap)
    }
  }

  def generateTableProperties(sqlTableName: String,
                              externalTable: CatalogTable,
                              isStream: Boolean): TableProperties = {

    val tableProperties = new TableProperties()
    tableProperties.addAll(externalTable.getProperties)
    isStream match {
      case true =>
        tableProperties.setString(
          TableProperties.BLINK_ENVIRONMENT_TYPE_KEY,
          TableProperties.BLINK_ENVIRONMENT_STREAM_VALUE)
      case false =>
        tableProperties.setString(
          TableProperties.BLINK_ENVIRONMENT_TYPE_KEY,
          TableProperties.BLINK_ENVIRONMENT_BATCHEXEC_VALUE)
    }
    // we choose table factory based on the connector type.
    tableProperties.setString(TableProperties.BLINK_CONNECTOR_TYPE_KEY, externalTable.getTableType)
    // put in internal arguments.
    tableProperties.putTableNameIntoProperties(sqlTableName)
    tableProperties.putSchemaIntoProperties(externalTable.getRichTableSchema)
    tableProperties
  }

  private def normalizeSupportedKeys(props: util.Map[String, String]): util.Map[String, String] = {
    val ret = new util.HashMap[String, String]()
    ret.putAll(props)
    TableProperties.INTERNAL_KEYS foreach(ret.remove(_))
    ret
  }

  private def getToolDescriptor(typeName: String, tableProperties: TableProperties)
    : ToolConnectorDescriptor = {
    new ToolConnectorDescriptor(typeName,
      normalizeSupportedKeys(tableProperties.toKeyLowerCase.toMap))
  }

  private def getStorageType (tableName: String, properties: TableProperties): String = {
    val typeName = properties.getString("type", null)
    Preconditions.checkState(typeName != null,
      "Property of table %s is missing", typeName)
    typeName
  }
}

class ToolConnectorDescriptor(typeName: String, properties: util.Map[String, String])
  extends ConnectorDescriptor(typeName, 1, false) {
  override protected def toConnectorProperties: util.Map[String, String] = {
    properties
  }
}
