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

package org.apache.flink.table.connectors

import java.util.{ServiceConfigurationError, ServiceLoader}

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator._
import org.apache.flink.table.descriptors.MetadataValidator._
import org.apache.flink.table.descriptors.StatisticsValidator._
import org.apache.flink.table.descriptors.{DescriptorProperties, TableDescriptor, TableDescriptorValidator}
import org.apache.flink.table.util.Logging

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * Unified interface to search for TableFactoryDiscoverable of provided type and properties.
  */
object TableFactoryService extends Logging {

  private lazy val defaultLoader = ServiceLoader.load(classOf[TableFactoryDiscoverable])

  def find(clz: Class[_], descriptor: TableDescriptor): TableFactoryDiscoverable = {
    find(clz, descriptor, null)
  }

  def find(clz: Class[_], descriptor: TableDescriptor, classLoader: ClassLoader)
  : TableFactoryDiscoverable = {

    val properties = new DescriptorProperties()
    descriptor.addProperties(properties)
    find(clz, properties.asMap.asScala.toMap, classLoader)
  }

  def find(clz: Class[_], properties: Map[String, String]): TableFactoryDiscoverable = {
    find(clz: Class[_], properties, null)
  }

  def find(clz: Class[_], properties: Map[String, String],
           classLoader: ClassLoader): TableFactoryDiscoverable = {

    var matchingFactory: Option[(TableFactoryDiscoverable, Seq[String])] = None
    try {
      val iter = if (classLoader == null) {
        defaultLoader.iterator()
      } else {
        val customLoader = ServiceLoader.load(classOf[TableFactoryDiscoverable], classLoader)
        customLoader.iterator()
      }
      while (iter.hasNext) {
        val factory = iter.next()

        if (clz.isAssignableFrom(factory.getClass)) {
          val requiredContextJava = try {
            factory.requiredContext()
          } catch {
            case t: Throwable =>
              throw new TableException(
                s"Table source factory '${factory.getClass.getCanonicalName}' caused an exception.",
                t)
          }

          val requiredContext = if (requiredContextJava != null) {
            // normalize properties
            requiredContextJava.asScala.map(e => (e._1.toLowerCase, e._2))
          } else {
            Map[String, String]()
          }

          val plainContext = mutable.Map[String, String]()
          plainContext ++= requiredContext
          // we remove the versions for now until we have the first backwards compatibility case
          // with the version we can provide mappings in case the format changes
          plainContext.remove(CONNECTOR_PROPERTY_VERSION)
          plainContext.remove(FORMAT_PROPERTY_VERSION)
          plainContext.remove(METADATA_PROPERTY_VERSION)
          plainContext.remove(STATISTICS_PROPERTY_VERSION)

          if (plainContext.forall(e => properties.contains(e._1) && properties(e._1) == e._2)) {
            matchingFactory match {
              case Some(_) => throw new AmbiguousTableFactoryException(properties)
              case None => matchingFactory =
                Some((factory.asInstanceOf[TableFactoryDiscoverable], requiredContext.keys.toSeq))
            }
          }
        }
      }
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table factories.", e)
        throw new TableException("Could not load service provider for table factories.", e)
    }

    val (factory, context) = matchingFactory
      .getOrElse(throw new NoMatchingTableFactoryException(properties))

    val plainProperties = mutable.ArrayBuffer[String]()
    properties.keys.foreach { k =>
      // replace arrays with wildcard
      val key = k.replaceAll(".\\d+", ".#")
      // ignore context properties and duplicates
      if (!context.contains(key) && !plainProperties.contains(key)) {
        plainProperties += key
      }
    }

    val supportedPropertiesJava = try {
      factory.supportedProperties()
    } catch {
      case t: Throwable =>
        throw new TableException(
          s"Table source factory '${factory.getClass.getCanonicalName}' caused an exception.",
          t)
    }

    val supportedProperties = if (supportedPropertiesJava != null) {
      supportedPropertiesJava.asScala.map(_.toLowerCase)
    } else {
      Seq[String]()
    }

    // check for supported properties
    plainProperties.foreach { k =>
      if (!k.equals(TableDescriptorValidator.TABLE_TYPE) && !supportedProperties.contains(k)) {
        throw new ValidationException(
          s"Table factory '${factory.getClass.getCanonicalName}' does not support the " +
            s"property '$k'. Supported properties are: \n" +
            s"${supportedProperties.map(DescriptorProperties.toString).mkString("\n")}")
      }
    }

    // create the table connector
    try {
      factory
    } catch {
      case t: Throwable =>
        throw new TableException(
          s"Table connector factory '${factory.getClass.getCanonicalName}' caused an exception.",
          t)
    }
  }
}
