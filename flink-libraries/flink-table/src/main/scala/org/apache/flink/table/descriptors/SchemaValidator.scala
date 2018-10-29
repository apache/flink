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
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.{TableException, TableSchema, ValidationException}
import org.apache.flink.table.descriptors.RowtimeValidator._
import org.apache.flink.table.descriptors.SchemaValidator._
import org.apache.flink.table.sources.RowtimeAttributeDescriptor
import org.apache.flink.table.util.JavaScalaConversionUtil.{toJava, toScala}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Validator for [[Schema]].
  */
class SchemaValidator(
    isStreamEnvironment: Boolean,
    supportsSourceTimestamps: Boolean,
    supportsSourceWatermarks: Boolean)
  extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    val names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME)
    val types = properties.getIndexedProperty(SCHEMA, SCHEMA_TYPE)

    if (names.isEmpty && types.isEmpty) {
      throw new ValidationException(
        s"Could not find the required schema in property '$SCHEMA'.")
    }

    var proctimeFound = false

    for (i <- 0 until Math.max(names.size, types.size)) {
      properties
        .validateString(s"$SCHEMA.$i.$SCHEMA_NAME", false, 1)
      properties
        .validateType(s"$SCHEMA.$i.$SCHEMA_TYPE", false, false)
      properties
        .validateString(s"$SCHEMA.$i.$SCHEMA_FROM", true, 1)
      // either proctime or rowtime
      val proctime = s"$SCHEMA.$i.$SCHEMA_PROCTIME"
      val rowtime = s"$SCHEMA.$i.$ROWTIME"
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
        properties.validateBoolean(proctime, false)
        proctimeFound = properties.getBoolean(proctime)
        // no rowtime
        properties.validatePrefixExclusion(rowtime)
      } else if (properties.hasPrefix(rowtime)) {
        // check rowtime
        val rowtimeValidator = new RowtimeValidator(
          supportsSourceTimestamps,
          supportsSourceWatermarks,
          s"$SCHEMA.$i.")
        rowtimeValidator.validate(properties)
        // no proctime
        properties.validateExclusion(proctime)
      }
    }
  }
}

object SchemaValidator {

  /**
    * Prefix for schema-related properties.
    */
  val SCHEMA = "schema"

  val SCHEMA_NAME = "name"
  val SCHEMA_TYPE = "type"
  val SCHEMA_PROCTIME = "proctime"
  val SCHEMA_FROM = "from"

  /**
    * Returns keys for a
    * [[org.apache.flink.table.factories.TableFormatFactory.supportedProperties()]] method that
    * are accepted for schema derivation using [[deriveFormatFields(DescriptorProperties)]].
    */
  def getSchemaDerivationKeys: util.List[String] = {
    val keys = new util.ArrayList[String]()

    // schema
    keys.add(SCHEMA + ".#." + SCHEMA_TYPE)
    keys.add(SCHEMA + ".#." + SCHEMA_NAME)
    keys.add(SCHEMA + ".#." + SCHEMA_FROM)

    // time attributes
    keys.add(SCHEMA + ".#." + SCHEMA_PROCTIME)
    keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE)
    keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM)
    keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS)
    keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED)
    keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE)
    keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS)
    keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED)
    keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY)

    keys
  }

  // utilities

  /**
    * Finds the proctime attribute if defined.
    */
  def deriveProctimeAttribute(properties: DescriptorProperties): Optional[String] = {
    val names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME)

    for (i <- 0 until names.size) {
      val isProctime = toScala(
        properties.getOptionalBoolean(s"$SCHEMA.$i.$SCHEMA_PROCTIME"))
      isProctime.foreach { isSet =>
        if (isSet) {
          return toJava(names.asScala.get(s"$SCHEMA.$i.$SCHEMA_NAME"))
        }
      }
    }
    toJava(None)
  }

  /**
    * Finds the rowtime attributes if defined.
    */
  def deriveRowtimeAttributes(properties: DescriptorProperties)
    : util.List[RowtimeAttributeDescriptor] = {

    val names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME)

    var attributes = new mutable.ArrayBuffer[RowtimeAttributeDescriptor]()

    // check for rowtime in every field
    for (i <- 0 until names.size) {
      RowtimeValidator
        .getRowtimeComponents(properties, s"$SCHEMA.$i.")
        .foreach { case (extractor, strategy) =>
          // create descriptor
          attributes += new RowtimeAttributeDescriptor(
            properties.getString(s"$SCHEMA.$i.$SCHEMA_NAME"),
            extractor,
            strategy)
        }
    }

    attributes.asJava
  }

  /**
    * Derives the table schema for a table sink. A sink ignores a proctime attribute and
    * needs to track the origin of a rowtime field.
    *
    * @deprecated This method combines two separate concepts of table schema and field mapping.
    *             This should be split into two methods once we have support for
    *             the corresponding interfaces (see FLINK-9870).
    */
  @deprecated
  def deriveTableSinkSchema(properties: DescriptorProperties): TableSchema = {
    val builder = TableSchema.builder()

    val schema = properties.getTableSchema(SCHEMA)

    schema.getFieldNames.zip(schema.getFieldTypes).zipWithIndex.foreach { case ((n, t), i) =>
      val isProctime = properties
        .getOptionalBoolean(s"$SCHEMA.$i.$SCHEMA_PROCTIME")
        .orElse(false)
      val tsType = s"$SCHEMA.$i.$ROWTIME_TIMESTAMPS_TYPE"
      val isRowtime = properties.containsKey(tsType)
      if (!isProctime && !isRowtime) {
        // check for a aliasing
        val fieldName = properties.getOptionalString(s"$SCHEMA.$i.$SCHEMA_FROM")
          .orElse(n)
        builder.field(fieldName, t)
      }
      // only use the rowtime attribute if it references a field
      else if (isRowtime) {
        properties.getString(tsType) match {
          case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD =>
            val field = properties.getString(s"$SCHEMA.$i.$ROWTIME_TIMESTAMPS_FROM")
            builder.field(field, t)

          // other timestamp strategies require a reverse timestamp extractor to
          // insert the timestamp into the output
          case t@_ =>
            throw new TableException(
              s"Unsupported rowtime type '$t' for sink table schema. Currently " +
              s"only '$ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD' is supported for table sinks.")
        }
      }
    }

    builder.build()
  }

  /**
    * Finds a table source field mapping.
    *
    * @param properties The properties describing a schema.
    * @param inputType  The input type that a connector and/or format produces. This parameter
    *                   can be used to resolve a rowtime field against an input field.
    */
  def deriveFieldMapping(
      properties: DescriptorProperties,
      inputType: Optional[TypeInformation[_]])
    : util.Map[String, String] = {

    val mapping = mutable.Map[String, String]()

    val schema = properties.getTableSchema(SCHEMA)

    val columnNames = toScala(inputType) match {
      case Some(composite: CompositeType[_]) => composite.getFieldNames.toSeq
      case _ => Seq[String]()
    }

    // add all source fields first because rowtime might reference one of them
    columnNames.foreach(name => mapping.put(name, name))

    // add all schema fields first for implicit mappings
    schema.getFieldNames.foreach { name =>
      mapping.put(name, name)
    }

    val names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME)

    for (i <- 0 until names.size) {
      val name = properties.getString(s"$SCHEMA.$i.$SCHEMA_NAME")
      toScala(properties.getOptionalString(s"$SCHEMA.$i.$SCHEMA_FROM")) match {

        // add explicit mapping
        case Some(source) =>
          mapping.put(name, source)

        // implicit mapping or time
        case None =>
          val isProctime = properties
            .getOptionalBoolean(s"$SCHEMA.$i.$SCHEMA_PROCTIME")
            .orElse(false)
          val isRowtime = properties
            .containsKey(s"$SCHEMA.$i.$ROWTIME_TIMESTAMPS_TYPE")
          // remove proctime/rowtime from mapping
          if (isProctime || isRowtime) {
            mapping.remove(name)
          }
          // check for invalid fields
          else if (!columnNames.contains(name)) {
            throw new ValidationException(s"Could not map the schema field '$name' to a field " +
              s"from source. Please specify the source field from which it can be derived.")
          }
      }
    }

    mapping.toMap.asJava
  }

  /**
    * Finds the fields that can be used for a format schema (without time attributes).
    */
  def deriveFormatFields(properties: DescriptorProperties): TableSchema = {

    val builder = TableSchema.builder()

    val schema = properties.getTableSchema(SCHEMA)

    schema.getFieldNames.zip(schema.getFieldTypes).zipWithIndex.foreach { case ((n, t), i) =>
      val isProctime = properties
        .getOptionalBoolean(s"$SCHEMA.$i.$SCHEMA_PROCTIME")
        .orElse(false)
      val tsType = s"$SCHEMA.$i.$ROWTIME_TIMESTAMPS_TYPE"
      val isRowtime = properties.containsKey(tsType)
      if (!isProctime && !isRowtime) {
        // check for a aliasing
        val fieldName = properties.getOptionalString(s"$SCHEMA.$i.$SCHEMA_FROM")
          .orElse(n)
        builder.field(fieldName, t)
      }
      // only use the rowtime attribute if it references a field
      else if (isRowtime &&
          properties.getString(tsType) == ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD) {
        val field = properties.getString(s"$SCHEMA.$i.$ROWTIME_TIMESTAMPS_FROM")
        builder.field(field, t)
      }
    }

    builder.build()
  }
}
