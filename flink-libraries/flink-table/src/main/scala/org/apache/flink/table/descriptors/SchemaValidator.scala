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

package org.apache.flink.table.descriptors

import java.util
import java.util.Optional

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.descriptors.RowtimeValidator.{ROWTIME, ROWTIME_TIMESTAMPS_TYPE}
import org.apache.flink.table.descriptors.SchemaValidator._
import org.apache.flink.table.sources.RowtimeAttributeDescriptor

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Validator for [[Schema]].
  */
class SchemaValidator(isStreamEnvironment: Boolean = true) extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateInt(SCHEMA_PROPERTY_VERSION, isOptional = true, 0, Integer.MAX_VALUE)

    properties.validateEnumValues(
      SCHEMA_DERIVE_FIELDS,
      isOptional = true,
      Seq(SCHEMA_DERIVE_FIELDS_VALUE_SEQUENTIALLY, SCHEMA_DERIVE_FIELDS_VALUE_ALPHABETICALLY))

    val names = properties.getIndexedProperty(SCHEMA_FIELDS, SCHEMA_FIELDS_NAME)
    val types = properties.getIndexedProperty(SCHEMA_FIELDS, SCHEMA_FIELDS_TYPE)

    if (names.isEmpty && types.isEmpty && !properties.containsKey(SCHEMA_DERIVE_FIELDS)) {
      throw new ValidationException(
        s"Could not find the required schema in property '$SCHEMA'.")
    }

    var proctimeFound = false

    for (i <- 0 until Math.max(names.size, types.size)) {
      properties
        .validateString(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_NAME", isOptional = false, minLen = 1)
      properties
        .validateType(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_TYPE", isOptional = false)
      properties
        .validateString(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_FROM", isOptional = true, minLen = 1)
      // either proctime or rowtime
      val proctime = s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_PROCTIME"
      val rowtime = s"$SCHEMA_FIELDS.$i.$ROWTIME"
      if (properties.containsKey(proctime)) {
        // check the environment
        if (!isStreamEnvironment) {
          throw new ValidationException(
            s"Property '$proctime' is not allowed in a batch environment.")
        }
        // check for only one proctime attribute
        else if (proctimeFound) {
          throw new ValidationException("A proctime attribute must only be defined once.")
        }
        // check proctime
        properties.validateBoolean(proctime, isOptional = false)
        proctimeFound = properties.getBoolean(proctime).get
        // no rowtime
        properties.validatePrefixExclusion(rowtime)
      } else if (properties.hasPrefix(rowtime)) {
        // check rowtime
        val rowtimeValidator = new RowtimeValidator(s"$SCHEMA_FIELDS.$i.")
        rowtimeValidator.validate(properties)
        // no proctime
        properties.validateExclusion(proctime)
      }
    }
  }
}

object SchemaValidator {

  val SCHEMA = "schema"
  val SCHEMA_PROPERTY_VERSION = "schema.property-version"
  val SCHEMA_FIELDS = "schema.fields"
  val SCHEMA_FIELDS_NAME = "name"
  val SCHEMA_FIELDS_TYPE = "type"
  val SCHEMA_FIELDS_PROCTIME = "proctime"
  val SCHEMA_FIELDS_FROM = "from"
  val SCHEMA_DERIVE_FIELDS = "schema.derive-fields"
  val SCHEMA_DERIVE_FIELDS_VALUE_ALPHABETICALLY = "alphabetically"
  val SCHEMA_DERIVE_FIELDS_VALUE_SEQUENTIALLY = "sequentially"

  // utilities

  /**
    * Derives a schema from properties and source.
    */
  def deriveSchema(
      properties: DescriptorProperties,
      sourceSchema: Option[TableSchema])
    : TableSchema = {

    val builder = TableSchema.builder()

    val schema = properties.getTableSchema(SCHEMA_FIELDS)

    val derivationMode = properties.getString(SCHEMA_DERIVE_FIELDS)

    val sourceNamesAndTypes = derivationMode match {
      case Some(SCHEMA_DERIVE_FIELDS_VALUE_ALPHABETICALLY) if sourceSchema.isDefined =>
        // sort by name
        sourceSchema.get.getColumnNames
          .zip(sourceSchema.get.getTypes)
          .sortBy(_._1)

      case Some(SCHEMA_DERIVE_FIELDS_VALUE_SEQUENTIALLY) if sourceSchema.isDefined =>
        sourceSchema.get.getColumnNames.zip(sourceSchema.get.getTypes)

      case Some(_) =>
        throw new ValidationException("Derivation of fields is not supported from this source.")

      case None =>
        Array[(String, TypeInformation[_])]()
    }

    // add source fields
    sourceNamesAndTypes.foreach { case (n, t) =>
      builder.field(n, t)
    }

    // add schema fields
    schema.foreach { ts =>
      val schemaNamesAndTypes = ts.getColumnNames.zip(ts.getTypes)
      schemaNamesAndTypes.foreach { case (n, t) =>
          // do not allow overwriting
          if (sourceNamesAndTypes.exists(_._1 == n)) {
            throw new ValidationException(
              "Specified schema fields must not overwrite fields derived from the source.")
          }
          builder.field(n, t)
      }
    }

    builder.build()
  }

  /**
    * Derives a schema from properties and source.
    * This method is intended for Java code.
    */
  def deriveSchema(
      properties: DescriptorProperties,
      sourceSchema: Optional[TableSchema])
    : TableSchema = {
    deriveSchema(
      properties,
      Option(sourceSchema.orElse(null)))
  }

  /**
    * Finds the proctime attribute if defined.
    */
  def deriveProctimeAttribute(properties: DescriptorProperties): Option[String] = {
    val names = properties.getIndexedProperty(SCHEMA_FIELDS, SCHEMA_FIELDS_NAME)

    for (i <- 0 until names.size) {
      val isProctime = properties.getBoolean(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_PROCTIME")
      isProctime.foreach { isSet =>
        if (isSet) {
          return names.get(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_NAME")
        }
      }
    }
    None
  }

  /**
    * Finds the proctime attribute if defined.
    * This method is intended for Java code.
    */
  def deriveProctimeOptional(properties: DescriptorProperties): Optional[String] = {
    Optional.ofNullable(deriveProctimeAttribute(properties).orNull)
  }

  /**
    * Finds the rowtime attributes if defined.
    */
  def deriveRowtimeAttributes(properties: DescriptorProperties)
    : util.List[RowtimeAttributeDescriptor] = {

    val names = properties.getIndexedProperty(SCHEMA_FIELDS, SCHEMA_FIELDS_NAME)

    var attributes = new mutable.ArrayBuffer[RowtimeAttributeDescriptor]()

    // check for rowtime in every field
    for (i <- 0 until names.size) {
      RowtimeValidator
        .getRowtimeComponents(properties, s"$SCHEMA_FIELDS.$i.")
        .foreach { case (extractor, strategy) =>
          // create descriptor
          attributes += new RowtimeAttributeDescriptor(
            properties.getString(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_NAME").get,
            extractor,
            strategy)
        }
    }

    attributes.asJava
  }

  /**
    * Find a table source field mapping.
    * This method is intended for Java code.
    */
  def deriveFieldMapping(
      properties: DescriptorProperties,
      sourceSchema: Optional[TableSchema])
    : util.Map[String, String] = {
    deriveFieldMapping(properties, Option(sourceSchema.orElse(null))).asJava
  }

  /**
    * Find a table source field mapping.
    */
  def deriveFieldMapping(
      properties: DescriptorProperties,
      sourceSchema: Option[TableSchema])
    : Map[String, String] = {

    val mapping = mutable.Map[String, String]()

    val schema = deriveSchema(properties, sourceSchema)

    // add all schema fields first for implicit mappings
    schema.getColumnNames.foreach { name =>
      mapping.put(name, name)
    }

    val names = properties.getIndexedProperty(SCHEMA_FIELDS, SCHEMA_FIELDS_NAME)

    for (i <- 0 until names.size) {
      val name = properties.getString(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_NAME").get
      properties.getString(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_FROM") match {

        // add explicit mapping
        case Some(source) =>
          mapping.put(name, source)

        // implicit mapping or proctime
        case None =>
          // remove proctime/rowtime from mapping
          if (properties.getBoolean(s"$SCHEMA_FIELDS.$i.$SCHEMA_FIELDS_PROCTIME")
                .getOrElse(false) ||
              properties.containsKey(s"$SCHEMA_FIELDS.$i.$ROWTIME_TIMESTAMPS_TYPE")) {
            mapping.remove(name)
          }
          // check for invalid fields
          else if (sourceSchema.forall(s => !s.getColumnNames.contains(name))) {
            throw new ValidationException(s"Could not map the schema field '$name' to a field " +
              s"from source. Please specify the source field from which it can be derived.")
          }
      }
    }

    mapping.toMap
  }
}
