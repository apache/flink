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

import java.io.Serializable
import java.lang.{Boolean => JBoolean, Double => JDouble, Integer => JInt, Long => JLong}
import java.util
import java.util.regex.Pattern

import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.descriptors.DescriptorProperties.{NAME, TYPE, normalizeTableSchema}
import org.apache.flink.table.typeutils.TypeStringUtils
import org.apache.flink.util.InstantiationUtil
import org.apache.flink.util.Preconditions.checkNotNull

import scala.collection.mutable

import scala.collection.JavaConverters._

/**
  * Utility class for having a unified string-based representation of Table API related classes
  * such as [[TableSchema]], [[TypeInformation]], etc.
  *
  * @param normalizeKeys flag that indicates if keys should be normalized (this flag is
  *                      necessary for backwards compatibility)
  */
class DescriptorProperties(normalizeKeys: Boolean = true) {

  private val properties: mutable.Map[String, String] = new mutable.HashMap[String, String]()

  private def put(key: String, value: String): Unit = {
    if (properties.contains(key)) {
      throw new IllegalStateException("Property already present.")
    }
    if (normalizeKeys) {
      properties.put(key.toLowerCase, value)
    } else {
      properties.put(key, value)
    }
  }

  // for testing
  private[flink] def unsafePut(key: String, value: String): Unit = {
    properties.put(key, value)
  }

  // for testing
  private[flink] def unsafeRemove(key: String): Unit = {
    properties.remove(key)
  }

  def putProperties(properties: Map[String, String]): Unit = {
    properties.foreach { case (k, v) =>
      put(k, v)
    }
  }

  def putProperties(properties: java.util.Map[String, String]): Unit = {
    properties.asScala.foreach { case (k, v) =>
      put(k, v)
    }
  }

  def putClass(key: String, clazz: Class[_]): Unit = {
    checkNotNull(key)
    checkNotNull(clazz)
    val error = InstantiationUtil.checkForInstantiationError(clazz)
    if (error != null) {
      throw new ValidationException(s"Class '${clazz.getName}' is not supported: $error")
    }
    put(key, clazz.getName)
  }

  def putString(key: String, str: String): Unit = {
    checkNotNull(key)
    checkNotNull(str)
    put(key, str)
  }

  def putBoolean(key: String, b: Boolean): Unit = {
    checkNotNull(key)
    put(key, b.toString)
  }

  def putLong(key: String, l: Long): Unit = {
    checkNotNull(key)
    put(key, l.toString)
  }

  def putInt(key: String, i: Int): Unit = {
    checkNotNull(key)
    put(key, i.toString)
  }

  def putCharacter(key: String, c: Character): Unit = {
    checkNotNull(key)
    checkNotNull(c)
    put(key, c.toString)
  }

  def putTableSchema(key: String, schema: TableSchema): Unit = {
    putTableSchema(key, normalizeTableSchema(schema))
  }

  def putTableSchema(key: String, nameAndType: Seq[(String, String)]): Unit = {
    putIndexedFixedProperties(
      key,
      Seq(NAME, TYPE),
      nameAndType.map(t => Seq(t._1, t._2))
    )
  }

  /**
    * Adds an indexed sequence of properties (with sub-properties) under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG, schema.fields.1.name = test2
    *
    * The arity of each propertyValue must match the arity of propertyKeys.
    */
  def putIndexedFixedProperties(
      key: String,
      propertyKeys: Seq[String],
      propertyValues: Seq[Seq[String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertyValues)
    propertyValues.zipWithIndex.foreach { case (values, idx) =>
      if (values.lengthCompare(propertyKeys.size) != 0) {
        throw new ValidationException("Values must have same arity as keys.")
      }
      values.zipWithIndex.foreach { case (value, keyIdx) =>
          put(s"$key.$idx.${propertyKeys(keyIdx)}", value)
      }
    }
  }

  /**
    * Adds an indexed mapping of properties under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    *                             schema.fields.1.name = test2
    *
    * The arity of the propertySets can differ.
    */
  def putIndexedVariableProperties(
      key: String,
      propertySets: Seq[Map[String, String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertySets)
    propertySets.zipWithIndex.foreach { case (propertySet, idx) =>
      propertySet.foreach { case (k, v) =>
        put(s"$key.$idx.$k", v)
      }
    }
  }

  // ----------------------------------------------------------------------------------------------

  def getString(key: String): Option[String] = {
    properties.get(key)
  }

  def getCharacter(key: String): Option[Character] = getString(key) match {
    case Some(c) =>
      if (c.length != 1) {
        throw new ValidationException(s"The value of $key must only contain one character.")
      }
      Some(c.charAt(0))

    case None => None
  }

  def getBoolean(key: String): Option[Boolean] = getString(key) match {
    case Some(b) => Some(JBoolean.parseBoolean(b))

    case None => None
  }

  def getInt(key: String): Option[Int] = getString(key) match {
    case Some(l) => Some(JInt.parseInt(l))

    case None => None
  }

  def getLong(key: String): Option[Long] = getString(key) match {
    case Some(l) => Some(JLong.parseLong(l))

    case None => None
  }

  def getDouble(key: String): Option[Double] = getString(key) match {
    case Some(d) => Some(JDouble.parseDouble(d))

    case None => None
  }

  def getTableSchema(key: String): Option[TableSchema] = {
    // filter for number of columns
    val fieldCount = properties
      .filterKeys(k => k.startsWith(key) && k.endsWith(s".$NAME"))
      .size

    if (fieldCount == 0) {
      return None
    }

    // validate fields and build schema
    val schemaBuilder = TableSchema.builder()
    for (i <- 0 until fieldCount) {
      val name = s"$key.$i.$NAME"
      val tpe = s"$key.$i.$TYPE"
      schemaBuilder.field(
        properties.getOrElse(name, throw new ValidationException(s"Invalid table schema. " +
          s"Could not find name for field '$key.$i'.")
        ),
        TypeStringUtils.readTypeInfo(
          properties.getOrElse(tpe, throw new ValidationException(s"Invalid table schema. " +
          s"Could not find type for field '$key.$i'."))
        )
      )
    }
    Some(schemaBuilder.build())
  }

  // ----------------------------------------------------------------------------------------------

  def validateString(
      key: String,
      isOptional: Boolean,
      minLen: Int = 0, // inclusive
      maxLen: Int = Integer.MAX_VALUE) // inclusive
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      val len = properties(key).length
      if (len < minLen || len > maxLen) {
        throw new ValidationException(
          s"Property '$key' must have a length between $minLen and $maxLen but " +
            s"was: ${properties(key)}")
      }
    }
  }

  def validateInt(
      key: String,
      isOptional: Boolean,
      min: Int = Int.MinValue, // inclusive
      max: Int = Int.MaxValue) // inclusive
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      try {
        val value = Integer.parseInt(properties(key))
        if (value < min || value > max) {
          throw new ValidationException(s"Property '$key' must be an integer value between $min " +
            s"and $max but was: ${properties(key)}")
        }
      } catch {
        case _: NumberFormatException =>
          throw new ValidationException(
            s"Property '$key' must be an integer value but was: ${properties(key)}")
      }
    }
  }

  def validateLong(
      key: String,
      isOptional: Boolean,
      min: Long = Long.MinValue, // inclusive
      max: Long = Long.MaxValue) // inclusive
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      try {
        val value = JLong.parseLong(properties(key))
        if (value < min || value > max) {
          throw new ValidationException(s"Property '$key' must be a long value between $min " +
            s"and $max but was: ${properties(key)}")
        }
      } catch {
        case _: NumberFormatException =>
          throw new ValidationException(
            s"Property '$key' must be a long value but was: ${properties(key)}")
      }
    }
  }

  def validateValue(key: String, value: String, isOptional: Boolean): Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      if (properties(key) != value) {
        throw new ValidationException(
          s"Could not find required value '$value' for property '$key'.")
      }
    }
  }

  def validateBoolean(key: String, isOptional: Boolean): Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      val value = properties(key)
      if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
        throw new ValidationException(
          s"Property '$key' must be a boolean value (true/false) but was: $value")
      }
    }
  }

  def validateDouble(
      key: String,
      isOptional: Boolean,
      min: Double = Double.MinValue, // inclusive
      max: Double = Double.MaxValue) // inclusive
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      try {
        val value = JDouble.parseDouble(properties(key))
        if (value < min || value > max) {
          throw new ValidationException(s"Property '$key' must be a double value between $min " +
            s"and $max but was: ${properties(key)}")
        }
      } catch {
        case _: NumberFormatException =>
          throw new ValidationException(
            s"Property '$key' must be an double value but was: ${properties(key)}")
      }
    }
  }

  def validateTableSchema(key: String, isOptional: Boolean): Unit = {
    // filter for name columns
    val names = getIndexedProperty(key, NAME)
    // filter for type columns
    val types = getIndexedProperty(key, TYPE)
    if (names.isEmpty && types.isEmpty && !isOptional) {
      throw new ValidationException(
        s"Could not find the required schema for property '$key'.")
    }
    for (i <- 0 until Math.max(names.size, types.size)) {
      validateString(s"$key.$i.$NAME", isOptional = false, minLen = 1)
      validateType(s"$key.$i.$TYPE", isOptional = false)
    }
  }

  def validateEnum(
      key: String,
      isOptional: Boolean,
      enumToValidation: Map[String, () => Unit])
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      val value = properties(key)
      if (!enumToValidation.contains(value)) {
        throw new ValidationException(s"Unknown value for property '$key'. " +
          s"Supported values [${enumToValidation.keys.mkString(", ")}] but was: $value")
      } else {
        enumToValidation(value).apply() // run validation logic
      }
    }
  }

  def validateType(key: String, isOptional: Boolean): Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      TypeStringUtils.readTypeInfo(properties(key)) // throws validation exceptions
    }
  }

  def validatePrefixExclusion(prefix: String): Unit = {
    val invalidField = properties.find(_._1.startsWith(prefix))
    if (invalidField.isDefined) {
      throw new ValidationException(
        s"Property '${invalidField.get._1}' is not allowed in this context.")
    }
  }

  def validateExclusion(key: String): Unit = {
    if (properties.contains(key)) {
      throw new ValidationException(s"Property '$key' is not allowed in this context.")
    }
  }

  // ----------------------------------------------------------------------------------------------

  def getIndexedProperty(key: String, property: String): Map[String, String] = {
    val escapedKey = Pattern.quote(key)
    properties.filterKeys(k => k.matches(s"$escapedKey\\.\\d+\\.$property")).toMap
  }

  def contains(str: String): Boolean = {
    properties.exists(e => e._1.contains(str))
  }

  def hasPrefix(prefix: String): Boolean = {
    properties.exists(e => e._1.startsWith(prefix))
  }

  def asMap: Map[String, String] = {
    properties.toMap
  }
}

object DescriptorProperties {

  val TYPE = "type"
  val NAME = "name"

  // the string representation should be equal to SqlTypeName
  def normalizeTypeInfo(typeInfo: TypeInformation[_]): String = {
    checkNotNull(typeInfo)
    TypeStringUtils.writeTypeInfo(typeInfo)
  }

  def normalizeTableSchema(schema: TableSchema): Seq[(String, String)] = {
    schema.getColumnNames.zip(schema.getTypes).map { case (n, t) =>
      (n, normalizeTypeInfo(t))
    }
  }

  def serialize(obj: Serializable): String = {
    // test public accessibility
    val error = InstantiationUtil.checkForInstantiationError(obj.getClass)
    if (error != null) {
      throw new ValidationException(s"Class '${obj.getClass.getName}' is not supported: $error")
    }
    try {
      val byteArray = InstantiationUtil.serializeObject(obj)
      Base64.encodeBase64URLSafeString(byteArray)
    } catch {
      case e: Exception =>
        throw new ValidationException(
          s"Unable to serialize class '${obj.getClass.getCanonicalName}'.", e)
    }
  }
}
