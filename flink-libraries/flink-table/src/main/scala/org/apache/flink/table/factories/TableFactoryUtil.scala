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

package org.apache.flink.table.factories

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ToolConnectorDescriptor
import org.apache.flink.table.descriptors.Descriptor
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.TableProperties

/**
  * Utility for dealing with [[TableFactory]] using the [[TableFactoryService]].
  */
object TableFactoryUtil {

  /**
    * Returns a table source for a table environment.
    *
    * Note: This function is just for Java compatibility.
    */
  def findAndCreateTableSource[T](
    tableEnvironment: TableEnvironment,
    descriptor: Descriptor)
  : TableSource = {

    findAndCreateTableSource(tableEnvironment, descriptor, null)
  }

  /**
    * Returns a table source for a table environment.
    */
  def findAndCreateTableSource[T](
      tableEnvironment: TableEnvironment,
      descriptor: Descriptor,
      classLoader: ClassLoader = null)
    : TableSource = {

    val javaMap = descriptor.toProperties

    tableEnvironment match {
      case _: BatchTableEnvironment =>
        TableFactoryService
          .find(classOf[BatchTableSourceFactory[T]], javaMap, classLoader)
          .createBatchTableSource(javaMap)

      case _: StreamTableEnvironment =>
        TableFactoryService
          .find(classOf[StreamTableSourceFactory[T]], javaMap, classLoader)
          .createStreamTableSource(javaMap)

      case e@_ =>
        throw new TableException(s"Unsupported table environment: ${e.getClass.getName}")
    }
  }

  /**
    * Returns a table sink for a table environment.
    *
    * Note: This function is just for Java compatibility.
    */
  def findAndCreateTableSink[T](
    tableEnvironment: TableEnvironment,
    descriptor: Descriptor)
  : TableSink[T] = {

    findAndCreateTableSink(tableEnvironment, descriptor, null)
  }

  /**
    * Returns a table sink for a table environment.
    */
  def findAndCreateTableSink[T](
      tableEnvironment: TableEnvironment,
      descriptor: Descriptor,
      classLoader: ClassLoader = null)
    : TableSink[T] = {

    val javaMap = descriptor.toProperties

    tableEnvironment match {
      case _: BatchTableEnvironment =>
        try {
          TableFactoryService
            .find(classOf[BatchTableSinkFactory[T]], javaMap, classLoader)
            .createBatchTableSink(javaMap)
        } catch {
          case _: NoMatchingTableFactoryException =>
            // fall back to compatible table sink
            TableFactoryService
              .find(classOf[BatchCompatibleTableSinkFactory[T]], javaMap, classLoader)
              .createBatchCompatibleTableSink(javaMap)
        }

      case _: StreamTableEnvironment =>
        TableFactoryService
          .find(classOf[StreamTableSinkFactory[T]], javaMap, classLoader)
          .createStreamTableSink(javaMap)

      case e@_ =>
        throw new TableException(s"Unsupported table environment: ${e.getClass.getName}")
    }
  }

  def getDiscriptorFromTableProperties(
      tableProperties: TableProperties): ToolConnectorDescriptor = {

    // get table discriptor
    val typeName = tableProperties.getString("connector.type", "")
    tableProperties.remove("connector.type")
    if (typeName.trim.length == 0) {
      throw new TableException("Connector type should not be null!")
    }
    new ToolConnectorDescriptor(typeName, tableProperties.toMap)
  }
}
