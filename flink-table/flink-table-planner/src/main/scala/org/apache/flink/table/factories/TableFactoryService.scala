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

import java.util.{ServiceConfigurationError, ServiceLoader, Map => JMap}

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator._
import org.apache.flink.table.descriptors.MetadataValidator._
import org.apache.flink.table.descriptors.StatisticsValidator._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * Unified interface to search for a [[TableFactory]] of provided type and properties.
  */
object TableFactoryService extends Logging {

  private lazy val defaultLoader = ServiceLoader.load(classOf[TableFactory])

  /**
    * Finds a table factory of the given class and descriptor.
    *
    * @param factoryClass desired factory class
    * @param descriptor descriptor describing the factory configuration
    * @tparam T factory class type
    * @return the matching factory
    */
  def find[T](factoryClass: Class[T], descriptor: Descriptor): T = {
    Preconditions.checkNotNull(descriptor)

    findInternal(factoryClass, descriptor.toProperties, None)
  }

  /**
    * Finds a table factory of the given class, descriptor, and classloader.
    *
    * @param factoryClass desired factory class
    * @param descriptor descriptor describing the factory configuration
    * @param classLoader classloader for service loading
    * @tparam T factory class type
    * @return the matching factory
    */
  def find[T](factoryClass: Class[T], descriptor: Descriptor, classLoader: ClassLoader): T = {
    Preconditions.checkNotNull(descriptor)
    Preconditions.checkNotNull(classLoader)

    findInternal(factoryClass, descriptor.toProperties, Some(classLoader))
  }

  /**
    * Finds a table factory of the given class and property map.
    *
    * @param factoryClass desired factory class
    * @param propertyMap properties that describe the factory configuration
    * @tparam T factory class type
    * @return the matching factory
    */
  def find[T](factoryClass: Class[T], propertyMap: JMap[String, String]): T = {
    findInternal(factoryClass, propertyMap, None)
  }

  /**
    * Finds a table factory of the given class, property map, and classloader.
    *
    * @param factoryClass desired factory class
    * @param propertyMap properties that describe the factory configuration
    * @param classLoader classloader for service loading
    * @tparam T factory class type
    * @return the matching factory
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
    * Finds a table factory of the given class, property map, and classloader.
    *
    * @param factoryClass desired factory class
    * @param propertyMap properties that describe the factory configuration
    * @param classLoader classloader for service loading
    * @tparam T factory class type
    * @return the matching factory
    */
  private def findInternal[T](
      factoryClass: Class[T],
      propertyMap: JMap[String, String],
      classLoader: Option[ClassLoader])
    : T = {

    Preconditions.checkNotNull(factoryClass)
    Preconditions.checkNotNull(propertyMap)

    val properties = propertyMap.asScala.toMap

    val foundFactories = discoverFactories(classLoader)

    val classFactories = filterByFactoryClass(
      factoryClass,
      properties,
      foundFactories)

    val contextFactories = filterByContext(
      factoryClass,
      properties,
      foundFactories,
      classFactories)

    filterBySupportedProperties(
      factoryClass,
      properties,
      foundFactories,
      contextFactories)
  }

  /**
    * Searches for factories using Java service providers.
    *
    * @return all factories in the classpath
    */
  private def discoverFactories[T](classLoader: Option[ClassLoader]): Seq[TableFactory] = {
    try {
      val iterator = classLoader match {
        case Some(customClassLoader) =>
          val customLoader = ServiceLoader.load(classOf[TableFactory], customClassLoader)
          customLoader.iterator()
        case None =>
          defaultLoader.iterator()
      }
      iterator.asScala.toSeq
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table factories.", e)
        throw new TableException("Could not load service provider for table factories.", e)
    }
  }

  /**
    * Filters factories with matching context by factory class.
    */
  private def filterByFactoryClass[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TableFactory])
    : Seq[TableFactory] = {

    val classFactories = foundFactories.filter(f => factoryClass.isAssignableFrom(f.getClass))
    if (classFactories.isEmpty) {
      throw new NoMatchingTableFactoryException(
        s"No factory implements '${factoryClass.getCanonicalName}'.",
        factoryClass,
        foundFactories,
        properties)
    }
    classFactories
  }

  /**
    * Filters for factories with matching context.
    *
    * @return all matching factories
    */
  private def filterByContext[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TableFactory],
      classFactories: Seq[TableFactory])
    : Seq[TableFactory] = {

    val matchingFactories = classFactories.filter { factory =>
      val requestedContext = normalizeContext(factory)

      val plainContext = mutable.Map[String, String]()
      plainContext ++= requestedContext
      // we remove the version for now until we have the first backwards compatibility case
      // with the version we can provide mappings in case the format changes
      plainContext.remove(CONNECTOR_PROPERTY_VERSION)
      plainContext.remove(FORMAT_PROPERTY_VERSION)
      plainContext.remove(METADATA_PROPERTY_VERSION)
      plainContext.remove(STATISTICS_PROPERTY_VERSION)

      // check if required context is met
      plainContext.forall(e => properties.contains(e._1) && properties(e._1) == e._2)
    }

    if (matchingFactories.isEmpty) {
      throw new NoMatchingTableFactoryException(
        "No context matches.",
        factoryClass,
        foundFactories,
        properties)
    }

    matchingFactories
  }

  /**
    * Prepares the properties of a context to be used for match operations.
    */
  private def normalizeContext(factory: TableFactory): Map[String, String] = {
    val requiredContextJava = factory.requiredContext()
    if (requiredContextJava == null) {
      throw new TableException(
        s"Required context of factory '${factory.getClass.getName}' must not be null.")
    }
    requiredContextJava.asScala.map(e => (e._1.toLowerCase, e._2)).toMap
  }

  /**
    * Filters the matching class factories by supported properties.
    */
  private def filterBySupportedProperties[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TableFactory],
      classFactories: Seq[TableFactory])
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
      val (supportedKeys, wildcards) = normalizeSupportedProperties(factory)
      // ignore context keys
      val givenContextFreeKeys = plainGivenKeys.filter(!requiredContextKeys.contains(_))
      // perform factory specific filtering of keys
      val givenFilteredKeys = filterSupportedPropertiesFactorySpecific(
        factory,
        givenContextFreeKeys)

      givenFilteredKeys.forall { k =>
        lastKey = Option(k)
        supportedKeys.contains(k) || wildcards.exists(k.startsWith)
      }
    }

    if (supportedFactories.isEmpty && classFactories.length == 1 && lastKey.isDefined) {
      // special case: when there is only one matching factory but the last property key
      // was incorrect
      val factory = classFactories.head
      val (supportedKeys, _) = normalizeSupportedProperties(factory)
      throw new NoMatchingTableFactoryException(
        s"""
          |The matching factory '${factory.getClass.getName}' doesn't support '${lastKey.get}'.
          |
          |Supported properties of this factory are:
          |${supportedKeys.sorted.mkString("\n")}""".stripMargin,
        factoryClass,
        foundFactories,
        properties)
    } else if (supportedFactories.isEmpty) {
      throw new NoMatchingTableFactoryException(
        s"No factory supports all properties.",
        factoryClass,
        foundFactories,
        properties)
    } else if (supportedFactories.length > 1) {
      throw new AmbiguousTableFactoryException(
        supportedFactories,
        factoryClass,
        foundFactories,
        properties)
    }

    supportedFactories.head.asInstanceOf[T]
  }

  /**
    * Prepares the supported properties of a factory to be used for match operations.
    */
  private def normalizeSupportedProperties(factory: TableFactory): (Seq[String], Seq[String]) = {
    val supportedPropertiesJava = factory.supportedProperties()
    if (supportedPropertiesJava == null) {
      throw new TableException(
        s"Supported properties of factory '${factory.getClass.getName}' must not be null.")
    }
    val supportedKeys = supportedPropertiesJava.asScala.map(_.toLowerCase)

    // extract wildcard prefixes
    val wildcards = extractWildcardPrefixes(supportedKeys)

    (supportedKeys, wildcards)
  }

  /**
    * Converts the prefix of properties with wildcards (e.g., "format.*").
    */
  private def extractWildcardPrefixes(propertyKeys: Seq[String]): Seq[String] = {
    propertyKeys
      .filter(_.endsWith("*"))
      .map(s => s.substring(0, s.length - 1))
  }

  /**
    * Performs filtering for special cases (i.e. table format factories with schema derivation).
    */
  private def filterSupportedPropertiesFactorySpecific(
      factory: TableFactory,
      keys: Seq[String])
    : Seq[String] = factory match {

    case formatFactory: TableFormatFactory[_] =>
      val includeSchema = formatFactory.supportsSchemaDerivation()
      // ignore non-format (or schema) keys
      keys.filter { k =>
        if (includeSchema) {
          k.startsWith(SchemaValidator.SCHEMA + ".") ||
            k.startsWith(FormatDescriptorValidator.FORMAT + ".")
        } else {
          k.startsWith(FormatDescriptorValidator.FORMAT + ".")
        }
      }

    case _ =>
      keys
  }
}
