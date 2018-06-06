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

package org.apache.flink.table.sources

import java.util.{ServiceConfigurationError, ServiceLoader}

import org.apache.flink.table.api.{AmbiguousTableSourceException, NoMatchingTableSourceException, TableException, ValidationException}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION
import org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION
import org.apache.flink.table.descriptors.MetadataValidator.METADATA_PROPERTY_VERSION
import org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_PROPERTY_VERSION
import org.apache.flink.table.descriptors._
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Service provider interface for finding suitable table source factories for the given properties.
  */
object TableSourceFactoryService extends Logging {

  private lazy val defaultLoader = ServiceLoader.load(classOf[TableSourceFactory[_]])

  def findAndCreateTableSource(descriptor: TableSourceDescriptor): TableSource[_] = {
    findAndCreateTableSource(descriptor, null)
  }

  def findAndCreateTableSource(
      descriptor: TableSourceDescriptor,
      classLoader: ClassLoader)
    : TableSource[_] = {

    val properties = new DescriptorProperties()
    descriptor.addProperties(properties)
    findAndCreateTableSource(properties.asMap.asScala.toMap, classLoader)
  }

  def findAndCreateTableSource(properties: Map[String, String]): TableSource[_] = {
    findAndCreateTableSource(properties, null)
  }

  def findAndCreateTableSource(
      properties: Map[String, String],
      classLoader: ClassLoader)
    : TableSource[_] = {

    var matchingFactory: Option[(TableSourceFactory[_], Seq[String])] = None
    try {
      val iter = if (classLoader == null) {
        defaultLoader.iterator()
      } else {
        val customLoader = ServiceLoader.load(classOf[TableSourceFactory[_]], classLoader)
        customLoader.iterator()
      }
      while (iter.hasNext) {
        val factory = iter.next()

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

        // check if required context is met
        if (plainContext.forall(e => properties.contains(e._1) && properties(e._1) == e._2)) {
          matchingFactory match {
            case Some(_) => throw new AmbiguousTableSourceException(properties)
            case None => matchingFactory = Some((factory, requiredContext.keys.toSeq))
          }
        }
      }
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table source factories.", e)
        throw new TableException("Could not load service provider for table source factories.", e)
    }

    val (factory, context) = matchingFactory
      .getOrElse(throw new NoMatchingTableSourceException(properties))

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
      if (!supportedProperties.contains(k)) {
        throw new ValidationException(
          s"Table factory '${factory.getClass.getCanonicalName}' does not support the " +
          s"property '$k'. Supported properties are: \n" +
          s"${supportedProperties.map(DescriptorProperties.toString).mkString("\n")}")
      }
    }

    // create the table source
    try {
      factory.create(properties.asJava)
    } catch {
      case t: Throwable =>
        throw new TableException(
          s"Table source factory '${factory.getClass.getCanonicalName}' caused an exception.",
          t)
    }
  }
}
