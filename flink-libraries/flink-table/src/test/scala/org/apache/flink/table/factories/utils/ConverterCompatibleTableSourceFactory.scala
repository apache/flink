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

package org.apache.flink.table.factories.utils

import java.util

import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.factories.{BatchTableSourceFactory, StreamTableSourceFactory, TableFactory}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource, csv}
import org.apache.flink.table.util.TableProperties

import scala.collection.JavaConverters._

class ConverterCompatibleCsvTableSourceFactory
  extends TableFactory
  with StreamTableSourceFactory[BaseRow]
  with BatchTableSourceFactory[BaseRow] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, ConverterCompatibleCsvTableSourceFactory.CONNECTOR_TYPE_VALUE)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()

    properties.add("path")
    properties.add("fielddelim")
    properties.add("rowdelim")

    properties
  }

  override def createStreamTableSource(
      properties: util.Map[String, String]): StreamTableSource[BaseRow] = {
    createCsvTableSource(properties)
  }

  override def createBatchTableSource(
      properties: util.Map[String, String]): BatchTableSource[BaseRow] = {
    createCsvTableSource(properties)
  }

  def createCsvTableSource(properties: util.Map[String, String]): CsvTableSource = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)

    val richTableSchema = tableProperties.readSchemaFromProperties(null)

    val builder = new csv.CsvTableSource.Builder

    val params = properties.asScala
    params.get("path").foreach(builder.path)

    params.get("fielddelim").foreach(builder.fieldDelimiter)

    params.get("rowdelim").foreach(builder.lineDelimiter)

    params.get("quotecharacter").foreach(quoteStr =>
      if (quoteStr.length != 1) {
        throw new IllegalArgumentException("the value of param must only contain one character!")
      } else {
        builder.quoteCharacter(quoteStr.charAt(0))
      }
    )

    params.get("ignorefirstline").foreach(ignoreFirstLine =>
      if (ignoreFirstLine.toBoolean) {
        builder.ignoreFirstLine()
      }
    )

    params.get("ignorecomments").foreach(builder.commentPrefix)

    params.get("lenient").foreach(lenientStr =>
      if (lenientStr.toBoolean) {
        builder.ignoreParseErrors()
      }
    )

    params.get("emptycolumnasnull").foreach(v =>
      if (v.toBoolean) {
        builder.enableEmptyColumnAsNull()
      }
    )

    richTableSchema.getColumnNames
      .zip(richTableSchema.getColumnTypes)
      .foreach(field => builder.field(field._1, field._2))

    builder.build()
  }
}

object ConverterCompatibleCsvTableSourceFactory {
  val CONNECTOR_TYPE_VALUE = "CSV"
}
