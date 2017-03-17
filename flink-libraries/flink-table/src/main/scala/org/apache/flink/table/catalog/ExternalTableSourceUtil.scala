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

import java.io.IOException
import java.lang.reflect.Modifier
import java.net.URL
import java.util.Properties

import org.apache.flink.table.annotation.TableType
import org.apache.flink.table.api.{AmbiguousTableSourceConverterException, NoMatchedTableSourceConverterException}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource
import org.apache.flink.util.InstantiationUtil
import org.reflections.Reflections
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * The utility class is used to convert ExternalCatalogTable to TableSourceTable.
  */
object ExternalTableSourceUtil {

  // config file to specify scan package to search TableSourceConverter
  private val tableSourceConverterConfigFileName = "tableSourceConverter.properties"

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  // registered table type with the TableSourceConverter.
  // Key is table type name, Value is set of converter class.
  private val tableTypeToTableSourceConvertersClazz = {
    val registeredConverters =
      new mutable.HashMap[String, mutable.Set[Class[_ <: TableSourceConverter[_]]]]
          with mutable.MultiMap[String, Class[_ <: TableSourceConverter[_]]]
    // scan all config files to find TableSourceConverters which are annotationed with TableType.
    val resourceUrls = getClass.getClassLoader.getResources(tableSourceConverterConfigFileName)
    while (resourceUrls.hasMoreElements) {
      val url = resourceUrls.nextElement()
      parseScanPackageFromConfigFile(url) match {
        case Some(scanPackage) =>
          val clazzWithAnnotations = new Reflections(scanPackage)
              .getTypesAnnotatedWith(classOf[TableType])
          clazzWithAnnotations.asScala.foreach(clazz =>
            if (classOf[TableSourceConverter[_]].isAssignableFrom(clazz)) {
              if (Modifier.isAbstract(clazz.getModifiers()) ||
                  Modifier.isInterface(clazz.getModifiers)) {
                LOG.warn(s"Class ${clazz.getName} is annotated with TableType " +
                    s"but an abstract class or interface.")
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
        case None =>
          LOG.warn(s"Fail to get scan package from config file [$url].")
      }
    }
    registeredConverters
  }

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable): TableSourceTable[_] = {
    val tableType = externalCatalogTable.tableType
    val propertyKeys = externalCatalogTable.properties.keySet()
    tableTypeToTableSourceConvertersClazz.get(tableType) match {
      case Some(converterClasses) =>
        val matchedConverters = converterClasses.map(InstantiationUtil.instantiate(_))
            .filter(converter => propertyKeys.containsAll(converter.requiredProperties))
        if (matchedConverters.isEmpty) {
          LOG.error(s"Cannot find any TableSourceConverter binded to table type [$tableType]. " +
              s"Register TableSourceConverter via externalCatalogTable.properties file.")
          throw new NoMatchedTableSourceConverterException(tableType)
        } else if (matchedConverters.size > 1) {
          LOG.error(s"Finds more than one matched TableSourceConverter for type [$tableType], " +
              s"they are ${matchedConverters.map(_.getClass.getName)}")
          throw new AmbiguousTableSourceConverterException(tableType)
        } else {
          val convertedTableSource: TableSource[_] = matchedConverters.head
              .fromExternalCatalogTable(externalCatalogTable)
              .asInstanceOf[TableSource[_]]
          val flinkStatistic = if (externalCatalogTable.stats != null) {
            FlinkStatistic.of(externalCatalogTable.stats)
          } else {
            FlinkStatistic.UNKNOWN
          }
          new TableSourceTable(convertedTableSource, flinkStatistic)
        }
      case None =>
        LOG.error(s"Cannot find any TableSourceConverter binded to table type [$tableType]. " +
            s"Register TableSourceConverter via externalCatalogTable.properties file.")
        throw new NoMatchedTableSourceConverterException(tableType)
    }
  }

  private def parseScanPackageFromConfigFile(url: URL): Option[String] = {
    val properties = new Properties()
    try {
      properties.load(url.openStream())
      val scanPackage = properties.getProperty("scan.package")
      if (scanPackage == null || scanPackage.isEmpty) {
        LOG.warn(s"Config file $url does not contain scan package!")
        None
      } else {
        Some(scanPackage)
      }
    } catch {
      case e: IOException =>
        LOG.warn(s"Fail to open config file [$url]", e)
        None
    }
  }

}
