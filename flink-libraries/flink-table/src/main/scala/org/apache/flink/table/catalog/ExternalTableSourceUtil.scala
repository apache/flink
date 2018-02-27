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

import java.net.URL

import org.apache.commons.configuration.{ConfigurationException, ConversionException, PropertiesConfiguration}
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.table.annotation.TableType
import org.apache.flink.table.api._
import org.apache.flink.table.plan.schema.{BatchTableSourceTable, StreamTableSourceTable, TableSourceTable}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource, TableSource, TableSourceFactoryService}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.InstantiationUtil
import org.reflections.Reflections

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * The utility class is used to convert ExternalCatalogTable to TableSourceTable.
  */
object ExternalTableSourceUtil extends Logging {

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable(
      tableEnv: TableEnvironment,
      externalCatalogTable: ExternalCatalogTable)
    : TableSourceTable[_] = {

    // check for the legacy external catalog path
    if (externalCatalogTable.isLegacyTableType) {
      LOG.warn("External catalog tables based on TableType annotations are deprecated. " +
        "Please consider updating them to TableSourceFactories.")
      fromExternalCatalogTableType(externalCatalogTable)
    }
    // use the factory approach
    else {
      val source = TableSourceFactoryService.findAndCreateTableSource(externalCatalogTable)
      tableEnv match {
        // check for a batch table source in this batch environment
        case _: BatchTableEnvironment =>
          source match {
            case bts: BatchTableSource[_] =>
              new BatchTableSourceTable(
                bts,
                new FlinkStatistic(externalCatalogTable.getTableStats))
            case _ => throw new TableException(
              s"Found table source '${source.getClass.getCanonicalName}' is not applicable " +
                s"in a batch environment.")
          }
        // check for a stream table source in this streaming environment
        case _: StreamTableEnvironment =>
          source match {
            case sts: StreamTableSource[_] =>
              new StreamTableSourceTable(
                sts,
                new FlinkStatistic(externalCatalogTable.getTableStats))
            case _ => throw new TableException(
              s"Found table source '${source.getClass.getCanonicalName}' is not applicable " +
                s"in a streaming environment.")
          }
        case _ => throw new TableException("Unsupported table environment.")
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // NOTE: the following lines can be removed once we drop support for TableType
  // ----------------------------------------------------------------------------------------------

  // config file to specify scan package to search TableSourceConverter
  private val tableSourceConverterConfigFileName = "tableSourceConverter.properties"

  // registered table type with the TableSourceConverter.
  // Key is table type name, Value is set of converter class.
  private val tableTypeToTableSourceConvertersClazz = {
    val registeredConverters =
      new mutable.HashMap[String, mutable.Set[Class[_ <: TableSourceConverter[_]]]]
          with mutable.MultiMap[String, Class[_ <: TableSourceConverter[_]]]
    // scan all config files to find TableSourceConverters which are annotated with TableType.
    val resourceUrls = getClass.getClassLoader.getResources(tableSourceConverterConfigFileName)
    while (resourceUrls.hasMoreElements) {
      val url = resourceUrls.nextElement()
      val scanPackages = parseScanPackagesFromConfigFile(url)
      scanPackages.foreach(scanPackage => {
        val clazzWithAnnotations = new Reflections(scanPackage)
            .getTypesAnnotatedWith(classOf[TableType])
        clazzWithAnnotations.asScala.foreach(clazz =>
          if (classOf[TableSourceConverter[_]].isAssignableFrom(clazz)) {
            val errorInfo = InstantiationUtil.checkForInstantiationError(clazz)
            if (errorInfo != null) {
              LOG.warn(s"Class ${clazz.getName} is annotated with TableType, " +
                  s"but is not instantiable because $errorInfo.")
            } else {
              val tableTypeAnnotation: TableType =
                clazz.getAnnotation(classOf[TableType])
              val tableType = tableTypeAnnotation.value()
              val converterClazz = clazz.asInstanceOf[Class[_ <: TableSourceConverter[_]]]
              registeredConverters.addBinding(tableType, converterClazz)
              LOG.info(s"Registers the converter ${clazz.getName} to table type [$tableType]. ")
            }
          } else {
            LOG.warn(
              s"Class ${clazz.getName} is annotated with TableType, " +
                  s"but does not implement the TableSourceConverter interface.")
          }
        )
      })
    }
    registeredConverters
  }

  @VisibleForTesting
  private[flink] def injectTableSourceConverter(
    tableType: String,
    converterClazz: Class[_ <: TableSourceConverter[_]]) = {
    tableTypeToTableSourceConvertersClazz.addBinding(tableType, converterClazz)
  }

  /**
    * Parses scan package set from input config file
    *
    * @param url url of config file
    * @return scan package set
    */
  private def parseScanPackagesFromConfigFile(url: URL): Set[String] = {
    try {
      val config = new PropertiesConfiguration(url)
      config.setListDelimiter(',')
      config.getStringArray("scan.packages").filterNot(_.isEmpty).toSet
    } catch {
      case e: ConfigurationException =>
        LOG.warn(s"Error happened while loading the properties file [$url]", e)
        Set.empty
      case e1: ConversionException =>
        LOG.warn(s"Error happened while parsing 'scan.packages' field of properties file [$url]. " +
            s"The value is not a String or List of Strings.", e1)
        Set.empty
    }
  }

  @VisibleForTesting
  def fromExternalCatalogTableType(externalCatalogTable: ExternalCatalogTable)
    : TableSourceTable[_] = {

    val tableType = externalCatalogTable.tableType
    val propertyKeys = externalCatalogTable.properties.keySet()
    tableTypeToTableSourceConvertersClazz.get(tableType) match {
      case Some(converterClasses) =>
        val matchedConverters = converterClasses.map(InstantiationUtil.instantiate(_))
        if (matchedConverters.isEmpty) {
          LOG.error(s"Cannot find any TableSourceConverter binded to table type [$tableType]. " +
              s"Register TableSourceConverter via externalCatalogTable.properties file.")
          throw new NoMatchedTableSourceConverterException(tableType)
        }
        val filteredMatchedConverters = matchedConverters.filter(
          converter => propertyKeys.containsAll(converter.requiredProperties))
        if(filteredMatchedConverters.isEmpty) {
          LOG.error(s"Cannot find any matched TableSourceConverter for type [$tableType], " +
              s"because the required properties does not match.")
          throw new NoMatchedTableSourceConverterException(tableType)
        } else if (filteredMatchedConverters.size > 1) {
          LOG.error(s"Finds more than one matched TableSourceConverter for type [$tableType], " +
              s"they are ${filteredMatchedConverters.map(_.getClass.getName)}")
          throw new AmbiguousTableSourceConverterException(tableType)
        } else {
          val convertedTableSource: TableSource[_] = filteredMatchedConverters.head
              .fromExternalCatalogTable(externalCatalogTable)
              .asInstanceOf[TableSource[_]]
          val flinkStatistic = if (externalCatalogTable.stats != null) {
            FlinkStatistic.of(externalCatalogTable.stats)
          } else {
            FlinkStatistic.UNKNOWN
          }

          convertedTableSource match {
            case s: StreamTableSource[_] =>
              new StreamTableSourceTable(s, flinkStatistic)
            case b: BatchTableSource[_] =>
              new BatchTableSourceTable(b, flinkStatistic)
            case _ =>
              throw new TableException("Unknown TableSource type.")

          }
        }
      case None =>
        LOG.error(s"Cannot find any TableSourceConverter binded to table type [$tableType]. " +
            s"Register TableSourceConverter via externalCatalogTable.properties file.")
        throw new NoMatchedTableSourceConverterException(tableType)
    }
  }
}
