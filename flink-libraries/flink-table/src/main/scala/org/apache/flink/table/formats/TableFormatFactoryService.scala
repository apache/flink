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

package org.apache.flink.table.formats

import java.util.{ServiceConfigurationError, ServiceLoader, Map => JMap}

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION
import org.apache.flink.table.descriptors._
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * Service provider interface for finding a suitable [[TableFormatFactory]] for the
  * given properties.
  */
object TableFormatFactoryService extends Logging {

  private lazy val defaultLoader = ServiceLoader.load(classOf[TableFormatFactory[_]])

  /**
    * Finds a table format factory of the given class and creates configured instances from the
    * given property map.
    *
    * @param factoryClass desired format factory
    * @param propertyMap properties that describes the format
    * @tparam T factory class type
    * @return configured instance from factory
    */
  def find[T](factoryClass: Class[T], propertyMap: JMap[String, String]): T = {
    findInternal(factoryClass, propertyMap, None)
  }

  /**
    * Finds a table format factory of the given class and creates configured instances from the
    * given property map and classloader.
    *
    * @param factoryClass desired format factory
    * @param propertyMap properties that describes the format
    * @param classLoader classloader for service loading
    * @tparam T factory class type
    * @return configured instance from factory
    */
  def find[T](
      factoryClass: Class[T],
      propertyMap: JMap[String, String],
      classLoader: ClassLoader)
    : T = {
    Preconditions.checkNotNull(classLoader)
    findInternal(factoryClass, propertyMap, Some(classLoader))
  }

  /**
    * Finds a table format factory of the given class and creates configured instances from the
    * given property map and classloader.
    *
    * @param factoryClass desired format factory
    * @param propertyMap properties that describes the format
    * @param classLoader optional classloader for service loading
    * @tparam T factory class type
    * @return configured instance from factory
    */
  private def findInternal[T](
      factoryClass: Class[T],
      propertyMap: JMap[String, String],
      classLoader: Option[ClassLoader])
    : T = {

    Preconditions.checkNotNull(factoryClass)
    Preconditions.checkNotNull(propertyMap)

    val properties = propertyMap.asScala.toMap

    // find matching context
    val (foundFactories, contextFactories) = findMatchingContext(
      factoryClass,
      properties,
      classLoader)

    // filter by factory class
    val classFactories = filterByFactoryClass(
      factoryClass,
      properties,
      foundFactories,
      contextFactories)

    // filter by supported keys
    filterBySupportedProperties(
      factoryClass,
      properties,
      foundFactories,
      classFactories)
  }

  private def findMatchingContext[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      classLoader: Option[ClassLoader])
    : (Seq[TableFormatFactory[_]], Seq[TableFormatFactory[_]]) = {

    val foundFactories = mutable.ArrayBuffer[TableFormatFactory[_]]()
    val matchingFactories = mutable.ArrayBuffer[TableFormatFactory[_]]()

    try {
      val iter = classLoader match {
        case Some(customClassLoader) =>
          val customLoader = ServiceLoader.load(classOf[TableFormatFactory[_]], customClassLoader)
          customLoader.iterator()
        case None =>
          defaultLoader.iterator()
      }

      while (iter.hasNext) {
        val factory = iter.next()
        foundFactories += factory

        val requestedContext = normalizeContext(factory)

        val plainContext = mutable.Map[String, String]()
        plainContext ++= requestedContext
        // we remove the version for now until we have the first backwards compatibility case
        // with the version we can provide mappings in case the format changes
        plainContext.remove(FORMAT_PROPERTY_VERSION)

        // check if required context is met
        if (plainContext.forall(e => properties.contains(e._1) && properties(e._1) == e._2)) {
          matchingFactories += factory
        }
      }
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table format factories.", e)
        throw new TableException("Could not load service provider for table format factories.", e)
    }

    if (matchingFactories.isEmpty) {
      throw new NoMatchingTableFormatException(
        "No context matches.",
        factoryClass,
        foundFactories,
        properties)
    }

    (foundFactories, matchingFactories)
  }

  private def normalizeContext(factory: TableFormatFactory[_]): Map[String, String] = {
    val requiredContextJava = factory.requiredContext()
    if (requiredContextJava == null) {
      throw new TableException(
        s"Required context of format factory '${factory.getClass.getName}' must not be null.")
    }
    requiredContextJava.asScala.map(e => (e._1.toLowerCase, e._2)).toMap
  }

  private def filterByFactoryClass[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TableFormatFactory[_]],
      contextFactories: Seq[TableFormatFactory[_]])
    : Seq[TableFormatFactory[_]] = {

    val classFactories = contextFactories.filter(f => factoryClass.isAssignableFrom(f.getClass))
    if (classFactories.isEmpty) {
      throw new NoMatchingTableFormatException(
        s"No factory implements '${factoryClass.getCanonicalName}'.",
        factoryClass,
        foundFactories,
        properties)
    }
    classFactories
  }

  private def filterBySupportedProperties[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TableFormatFactory[_]],
      classFactories: Seq[TableFormatFactory[_]])
    : T = {

    val plainGivenKeys = mutable.ArrayBuffer[String]()
    properties.keys.foreach { k =>
      // replace arrays with wildcard
      val key = k.replaceAll(".\\d+", ".#")
      // ignore duplicates
      if (!plainGivenKeys.contains(key)) {
        plainGivenKeys += key
      }
    }
    var lastKey: Option[String] = None
    val supportedFactories = classFactories.filter { factory =>
      val requiredContextKeys = normalizeContext(factory).keySet
      val includeSchema = factory.supportsSchemaDerivation()
      val supportedKeys = normalizeSupportedProperties(factory)
      val givenKeys = plainGivenKeys
        // ignore context keys
        .filter(!requiredContextKeys.contains(_))
        // ignore non-format (or schema) keys
        .filter { k =>
          if (includeSchema) {
            k.startsWith(SchemaValidator.SCHEMA + ".") ||
              k.startsWith(FormatDescriptorValidator.FORMAT + ".")
          } else {
            k.startsWith(FormatDescriptorValidator.FORMAT + ".")
          }
        }
      givenKeys.forall { k =>
        lastKey = Option(k)
        supportedKeys.contains(k)
      }
    }

    if (supportedFactories.isEmpty && classFactories.length == 1 && lastKey.isDefined) {
      // special case: when there is only one matching factory but the last property key
      // was incorrect
      val factory = classFactories.head
      val supportedKeys = normalizeSupportedProperties(factory)
      throw new NoMatchingTableFormatException(
        s"""
          |The matching factory '${factory.getClass.getName}' doesn't support '${lastKey.get}'.
          |
          |Supported properties of this factory are:
          |${supportedKeys.sorted.mkString("\n")}""".stripMargin,
        factoryClass,
        foundFactories,
        properties)
    } else if (supportedFactories.isEmpty) {
      throw new NoMatchingTableFormatException(
        s"No factory supports all properties.",
        factoryClass,
        foundFactories,
        properties)
    } else if (supportedFactories.length > 1) {
      throw new AmbiguousTableFormatException(
        supportedFactories,
        factoryClass,
        foundFactories,
        properties)
    }

    supportedFactories.head.asInstanceOf[T]
  }

  private def normalizeSupportedProperties(factory: TableFormatFactory[_]): Seq[String] = {
    val supportedPropertiesJava = factory.supportedProperties()
    if (supportedPropertiesJava == null) {
      throw new TableException(
        s"Supported properties of format factory '${factory.getClass.getName}' must not be null.")
    }
    supportedPropertiesJava.asScala.map(_.toLowerCase)
  }
}
