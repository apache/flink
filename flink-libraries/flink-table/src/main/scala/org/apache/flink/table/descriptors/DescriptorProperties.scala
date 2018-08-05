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
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.util
import java.util.function.{Consumer, Supplier}
import java.util.regex.Pattern
import java.util.{Optional, List => JList, Map => JMap}

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringEscapeUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableException, TableSchema, ValidationException}
import org.apache.flink.table.descriptors.DescriptorProperties.{NAME, TYPE, normalizeTableSchema, toJava}
import org.apache.flink.table.typeutils.TypeStringUtils
import org.apache.flink.util.InstantiationUtil
import org.apache.flink.util.Preconditions.checkNotNull

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Utility class for having a unified string-based representation of Table API related classes
  * such as [[TableSchema]], [[TypeInformation]], etc.
  *
  * '''Note to implementers''': Please try to reuse key names as much as possible. Key-names
  * should be hierarchical and lower case. Use "-" instead of dots or camel case.
  * E.g., connector.schema.start-from = from-earliest. Try not to use the higher level in a
  * key-name. E.g., instead of connector.kafka.kafka-version use connector.kafka.version.
  *
  * @param normalizeKeys flag that indicates if keys should be normalized (this flag is
  *                      necessary for backwards compatibility)
  */
class DescriptorProperties(normalizeKeys: Boolean = true) {

  private val properties: mutable.Map[String, String] = new mutable.HashMap[String, String]()

  /**
    * Adds a set of properties.
    */
  def putProperties(properties: JMap[String, String]): Unit = {
    properties.asScala.foreach { case (k, v) =>
      put(k, v)
    }
  }

  /**
    * Adds a class under the given key.
    */
  def putClass(key: String, clazz: Class[_]): Unit = {
    checkNotNull(key)
    checkNotNull(clazz)
    val error = InstantiationUtil.checkForInstantiationError(clazz)
    if (error != null) {
      throw new ValidationException(s"Class '${clazz.getName}' is not supported: $error")
    }
    put(key, clazz.getName)
  }

  /**
    * Adds a string under the given key.
    */
  def putString(key: String, str: String): Unit = {
    checkNotNull(key)
    checkNotNull(str)
    put(key, str)
  }

  /**
    * Adds a boolean under the given key.
    */
  def putBoolean(key: String, b: Boolean): Unit = {
    checkNotNull(key)
    put(key, b.toString)
  }

  /**
    * Adds a long under the given key.
    */
  def putLong(key: String, l: Long): Unit = {
    checkNotNull(key)
    put(key, l.toString)
  }

  /**
    * Adds an integer under the given key.
    */
  def putInt(key: String, i: Int): Unit = {
    checkNotNull(key)
    put(key, i.toString)
  }

  /**
    * Adds a character under the given key.
    */
  def putCharacter(key: String, c: Character): Unit = {
    checkNotNull(key)
    checkNotNull(c)
    put(key, c.toString)
  }

  /**
    * Adds a table schema under the given key.
    */
  def putTableSchema(key: String, schema: TableSchema): Unit = {
    checkNotNull(key)
    checkNotNull(schema)
    putTableSchema(key, normalizeTableSchema(schema))
  }

  /**
    * Adds a table schema under the given key.
    */
  def putTableSchema(key: String, nameAndType: JList[JTuple2[String, String]]): Unit = {
    putTableSchema(key, nameAndType.asScala.map(t => (t.f0, t.f1)))
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
      propertyKeys: JList[String],
      propertyValues: JList[JList[String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertyValues)
    putIndexedFixedProperties(key, propertyKeys.asScala, propertyValues.asScala.map(_.asScala))
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
      propertySets: JList[JMap[String, String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertySets)
    putIndexedVariableProperties(key, propertySets.asScala.map(_.asScala.toMap))
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns a string value under the given key if it exists.
    */
  def getOptionalString(key: String): Optional[String] = toJava(properties.get(key))

  /**
    * Returns a string value under the given existing key.
    */
  def getString(key: String): String = {
    get(key)
  }

  /**
    * Returns a character value under the given key if it exists.
    */
  def getOptionalCharacter(key: String): Optional[Character] = {
    val value = properties.get(key).map { c =>
      if (c.length != 1) {
        throw new ValidationException(s"The value of $key must only contain one character.")
      }
      Char.box(c.charAt(0))
    }
    toJava(value)
  }

  /**
    * Returns a character value under the given existing key.
    */
  def getCharacter(key: String): Char = {
    getOptionalCharacter(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a class value under the given key if it exists.
    */
  def getOptionalClass[T](key: String, superClass: Class[T]): Optional[Class[T]] = {
    val value = properties.get(key).map { name =>
      val clazz = try {
        Class.forName(
          name,
          true,
          Thread.currentThread().getContextClassLoader).asInstanceOf[Class[T]]
      } catch {
        case e: Exception =>
          throw new ValidationException(s"Could not get class '$name' for key '$key'.", e)
      }
      if (!superClass.isAssignableFrom(clazz)) {
        throw new ValidationException(s"Class '$name' does not extend from the required " +
          s"class '${superClass.getName}' for key '$key'.")
      }
      clazz
    }
    toJava(value)
  }

  /**
    * Returns a class value under the given existing key.
    */
  def getClass[T](key: String, superClass: Class[T]): Class[T] = {
    getOptionalClass(key, superClass).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a big decimal value under the given key if it exists.
    */
  def getOptionalBigDecimal(key: String): Optional[JBigDecimal] = {
    val value = properties.get(key).map(new JBigDecimal(_))
    toJava(value)
  }

  /**
    * Returns a big decimal value under the given existing key.
    */
  def getBigDecimal(key: String): JBigDecimal = {
    getOptionalBigDecimal(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a boolean value under the given key if it exists.
    */
  def getOptionalBoolean(key: String): Optional[JBoolean] = {
    val value = properties.get(key).map(JBoolean.parseBoolean).map(Boolean.box)
    toJava(value)
  }

  /**
    * Returns a boolean value under the given existing key.
    */
  def getBoolean(key: String): Boolean = {
    getOptionalBoolean(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a byte value under the given key if it exists.
    */
  def getOptionalByte(key: String): Optional[JByte] = {
    val value = properties.get(key).map(JByte.parseByte).map(Byte.box)
    toJava(value)
  }

  /**
    * Returns a byte value under the given existing key.
    */
  def getByte(key: String): Byte = {
    getOptionalByte(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a double value under the given key if it exists.
    */
  def getOptionalDouble(key: String): Optional[JDouble] = {
    val value = properties.get(key).map(JDouble.parseDouble).map(Double.box)
    toJava(value)
  }

  /**
    * Returns a double value under the given existing key.
    */
  def getDouble(key: String): Double = {
    getOptionalDouble(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a float value under the given key if it exists.
    */
  def getOptionalFloat(key: String): Optional[JFloat] = {
    val value = properties.get(key).map(JFloat.parseFloat).map(Float.box)
    toJava(value)
  }

  /**
    * Returns a float value under the given given existing key.
    */
  def getFloat(key: String): Float = {
    getOptionalFloat(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns an integer value under the given key if it exists.
    */
  def getOptionalInt(key: String): Optional[JInt] = {
    val value = properties.get(key).map(JInt.parseInt).map(Int.box)
    toJava(value)
  }

  /**
    * Returns an integer value under the given existing key.
    */
  def getInt(key: String): Int = {
    getOptionalInt(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a long value under the given key if it exists.
    */
  def getOptionalLong(key: String): Optional[JLong] = {
    val value = properties.get(key).map(JLong.parseLong).map(Long.box)
    toJava(value)
  }

  /**
    * Returns a long value under the given existing key.
    */
  def getLong(key: String): Long = {
    getOptionalLong(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a short value under the given key if it exists.
    */
  def getOptionalShort(key: String): Optional[JShort] = {
    val value = properties.get(key).map(JShort.parseShort).map(Short.box)
    toJava(value)
  }

  /**
    * Returns a short value under the given existing key.
    */
  def getShort(key: String): Short = {
    getOptionalShort(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns the type information under the given key if it exists.
    */
  def getOptionalType(key: String): Optional[TypeInformation[_]] = {
    val value = properties.get(key).map(TypeStringUtils.readTypeInfo)
    toJava(value)
  }

  /**
    * Returns the type information under the given existing key.
    */
  def getType(key: String): TypeInformation[_] = {
    getOptionalType(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns a table schema under the given key if it exists.
    */
  def getOptionalTableSchema(key: String): Optional[TableSchema] = {
    // filter for number of columns
    val fieldCount = properties
      .filterKeys(k => k.startsWith(key) && k.endsWith(s".$NAME"))
      .size

    if (fieldCount == 0) {
      return toJava(None)
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
    toJava(Some(schemaBuilder.build()))
  }

  /**
    * Returns a table schema under the given existing key.
    */
  def getTableSchema(key: String): TableSchema = {
    getOptionalTableSchema(key).orElseThrow(exceptionSupplier(key))
  }

  /**
    * Returns the property keys of fixed indexed properties.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG, schema.fields.1.name = test2
    *
    * getFixedIndexedProperties("schema.fields", List("type", "name")) leads to:
    *
    * 0: Map("type" -> "schema.fields.0.type", "name" -> "schema.fields.0.name")
    * 1: Map("type" -> "schema.fields.1.type", "name" -> "schema.fields.1.name")
    */
  def getFixedIndexedProperties(
      key: String,
      propertyKeys: JList[String])
    : JList[JMap[String, String]] = {

    val keys = propertyKeys.asScala

    // filter for index
    val escapedKey = Pattern.quote(key)
    val pattern = Pattern.compile(s"$escapedKey\\.(\\d+)\\.(.*)")

    // extract index and property keys
    val indexes = properties.keys.flatMap { k =>
      val matcher = pattern.matcher(k)
      if (matcher.find()) {
        Some(JInt.parseInt(matcher.group(1)))
      } else {
        None
      }
    }

    // determine max index
    val maxIndex = indexes.reduceOption(_ max _).getOrElse(-1)

    // validate and create result
    val list = new util.ArrayList[JMap[String, String]]()
    for (i <- 0 to maxIndex) {
      val map = new util.HashMap[String, String]()

      keys.foreach { subKey =>
        val fullKey = s"$key.$i.$subKey"
        // check for existence of full key
        if (!containsKey(fullKey)) {
          throw exceptionSupplier(fullKey).get()
        }
        map.put(subKey, fullKey)
      }

      list.add(map)
    }
    list
  }

  /**
    * Returns the property keys of variable indexed properties.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG
    *
    * getFixedIndexedProperties("schema.fields", List("type")) leads to:
    *
    * 0: Map("type" -> "schema.fields.0.type", "name" -> "schema.fields.0.name")
    * 1: Map("type" -> "schema.fields.1.type")
    */
  def getVariableIndexedProperties(
      key: String,
      requiredKeys: JList[String])
    : JList[JMap[String, String]] = {

    val keys = requiredKeys.asScala

    // filter for index
    val escapedKey = Pattern.quote(key)
    val pattern = Pattern.compile(s"$escapedKey\\.(\\d+)(\\.)?(.*)")

    // extract index and property keys
    val indexes = properties.keys.flatMap { k =>
      val matcher = pattern.matcher(k)
      if (matcher.find()) {
        Some((JInt.parseInt(matcher.group(1)), matcher.group(3)))
      } else {
        None
      }
    }

    // determine max index
    val maxIndex = indexes.map(_._1).reduceOption(_ max _).getOrElse(-1)

    // validate and create result
    val list = new util.ArrayList[JMap[String, String]]()
    for (i <- 0 to maxIndex) {
      val map = new util.HashMap[String, String]()

      // check and add required keys
      keys.foreach { subKey =>
        val fullKey = s"$key.$i.$subKey"
        // check for existence of full key
        if (!containsKey(fullKey)) {
          throw exceptionSupplier(fullKey).get()
        }
        map.put(subKey, fullKey)
      }

      // add optional keys
      indexes.filter(_._1 == i).foreach { case (_, subKey) =>
        val fullKey = s"$key.$i.$subKey"
        map.put(subKey, fullKey)
      }

      list.add(map)
    }
    list
  }

  /**
    * Returns all properties under a given key that contains an index in between.
    *
    * E.g. rowtime.0.name -> returns all rowtime.#.name properties
    */
  def getIndexedProperty(key: String, property: String): JMap[String, String] = {
    val escapedKey = Pattern.quote(key)
    properties.filterKeys(k => k.matches(s"$escapedKey\\.\\d+\\.$property")).asJava
  }

  /**
    * Returns a prefix subset of properties.
    */
  def getPrefix(prefixKey: String): JMap[String, String] = {
    val prefix = prefixKey + '.'
    properties.filterKeys(_.startsWith(prefix)).toSeq.map{ case (k, v) =>
      k.substring(prefix.length) -> v // remove prefix
    }.toMap.asJava
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Validates a string property.
    */
  def validateString(
      key: String,
      isOptional: Boolean)
    : Unit = {
    validateString(key, isOptional, 0, Integer.MAX_VALUE)
  }

  /**
    * Validates a string property. The boundaries are inclusive.
    */
  def validateString(
      key: String,
      isOptional: Boolean,
      minLen: Int) // inclusive
    : Unit = {
    validateString(key, isOptional, minLen, Integer.MAX_VALUE)
  }

  /**
    * Validates a string property. The boundaries are inclusive.
    */
  def validateString(
      key: String,
      isOptional: Boolean,
      minLen: Int, // inclusive
      maxLen: Int) // inclusive
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

  /**
    * Validates an integer property.
    */
  def validateInt(
      key: String,
      isOptional: Boolean)
    : Unit = {
    validateInt(key, isOptional, Int.MinValue, Int.MaxValue)
  }

  /**
    * Validates an integer property. The boundaries are inclusive.
    */
  def validateInt(
      key: String,
      isOptional: Boolean,
      min: Int) // inclusive
    : Unit = {
    validateInt(key, isOptional, min, Int.MaxValue)
  }

  /**
    * Validates an integer property. The boundaries are inclusive.
    */
  def validateInt(
      key: String,
      isOptional: Boolean,
      min: Int, // inclusive
      max: Int) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new Integer(min), new Integer(max), Integer.valueOf)
  }

  /**
    * Validates a long property.
    */
  def validateLong(
      key: String,
      isOptional: Boolean)
    : Unit = {
    validateLong(key, isOptional, Long.MinValue, Long.MaxValue)
  }

  /**
    * Validates a long property. The boundaries are inclusive.
    */
  def validateLong(
      key: String,
      isOptional: Boolean,
      min: Long) // inclusive
    : Unit = {
    validateLong(key, isOptional, min, Long.MaxValue)
  }

  /**
    * Validates a long property. The boundaries are inclusive.
    */
  def validateLong(
      key: String,
      isOptional: Boolean,
      min: Long, // inclusive
      max: Long) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new JLong(min), new JLong(max), JLong.valueOf)
  }

  /**
    * Validates that a certain value is present under the given key.
    */
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

  /**
    * Validates that a boolean value is present under the given key.
    */
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

  /**
    * Validates a double property.
    */
  def validateDouble(
      key: String,
      isOptional: Boolean)
    : Unit = {
    validateDouble(key, isOptional, Double.MinValue, Double.MaxValue)
  }

  /**
    * Validates a double property. The boundaries are inclusive.
    */
  def validateDouble(
      key: String,
      isOptional: Boolean,
      min: Double) // inclusive
    : Unit = {
    validateDouble(key, isOptional, min, Double.MaxValue)
  }

  /**
    * Validates a double property. The boundaries are inclusive.
    */
  def validateDouble(
      key: String,
      isOptional: Boolean,
      min: Double, // inclusive
      max: Double) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new JDouble(min), new JDouble(max), JDouble.valueOf)
  }

  /**
    * Validates a big decimal property.
    */
  def validateBigDecimal(
      key: String,
      isOptional: Boolean)
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      try {
        new JBigDecimal(properties(key))
      } catch {
        case _: NumberFormatException =>
          throw new ValidationException(
            s"Property '$key' must be a big decimal value but was: ${properties(key)}")
      }
    }
  }

  /**
    * Validates a big decimal property. The boundaries are inclusive.
    */
  def validateBigDecimal(
      key: String,
      isOptional: Boolean,
      min: JBigDecimal, // inclusive
      max: JBigDecimal) // inclusive
    : Unit = {
    validateComparable(
      key,
      isOptional,
      min,
      max,
      (value: String) => new JBigDecimal(value))
  }

  /**
    * Validates a byte property.
    */
  def validateByte(
      key: String,
      isOptional: Boolean): Unit = validateByte(key, isOptional, Byte.MinValue, Byte.MaxValue)

  /**
    * Validates a byte property. The boundaries are inclusive.
    */
  def validateByte(
      key: String,
      isOptional: Boolean,
      min: Byte) // inclusive
    : Unit = {
    validateByte(key, isOptional, min, Byte.MaxValue)
  }

  /**
    * Validates a byte property. The boundaries are inclusive.
    */
  def validateByte(
      key: String,
      isOptional: Boolean,
      min: Byte, // inclusive
      max: Byte) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new JByte(min), new JByte(max), JByte.valueOf)
  }

  /**
    * Validates a float property.
    */
  def validateFloat(
      key: String,
      isOptional: Boolean): Unit = validateFloat(key, isOptional, Float.MinValue, Float.MaxValue)

  /**
    * Validates a float property. The boundaries are inclusive.
    */
  def validateFloat(
      key: String,
      isOptional: Boolean,
      min: Float) // inclusive
    : Unit = {
    validateFloat(key, isOptional, min, Float.MaxValue)
  }

  /**
    * Validates a float property. The boundaries are inclusive.
    */
  def validateFloat(
      key: String,
      isOptional: Boolean,
      min: Float, // inclusive
      max: Float) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new JFloat(min), new JFloat(max), JFloat.valueOf)
  }

  /**
    * Validates a short property.
    */
  def validateShort(
      key: String,
      isOptional: Boolean): Unit = validateFloat(key, isOptional, Short.MinValue, Short.MaxValue)

  /**
    * Validates a short property. The boundaries are inclusive.
    */
  def validateShort(
      key: String,
      isOptional: Boolean,
      min: Short) // inclusive
    : Unit = {
    validateShort(key, isOptional, min, Short.MaxValue)
  }

  /**
    * Validates a short property. The boundaries are inclusive.
    */
  def validateShort(
      key: String,
      isOptional: Boolean,
      min: Short, // inclusive
      max: Short) // inclusive
    : Unit = {
    validateComparable(key, isOptional, new JShort(min), new JShort(max), JShort.valueOf)
  }

  /**
    * Validation for variable indexed properties.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG
    *
    * The propertyKeys map defines e.g. "type" and a validation logic for the given full key.
    *
    * The validation consumer takes the current prefix e.g. "schema.fields.1.".
    */
  def validateVariableIndexedProperties(
      key: String,
      allowEmpty: Boolean,
      propertyKeys: JMap[String, Consumer[String]],
      requiredKeys: JList[String])
    : Unit = {

    val keys = propertyKeys.asScala

    // filter for index
    val escapedKey = Pattern.quote(key)
    val pattern = Pattern.compile(s"$escapedKey\\.(\\d+)\\.(.*)")

    // extract index and property keys
    val indexes = properties.keys.flatMap { k =>
      val matcher = pattern.matcher(k)
      if (matcher.find()) {
        Some(JInt.parseInt(matcher.group(1)))
      } else {
        None
      }
    }

    // determine max index
    val maxIndex = indexes.reduceOption(_ max _).getOrElse(-1)

    if (maxIndex < 0 && !allowEmpty) {
      throw new ValidationException(s"Property key '$key' must not be empty.")
    }

    // validate
    for (i <- 0 to maxIndex) {
      keys.foreach { case (subKey, validation) =>
        val fullKey = s"$key.$i.$subKey"
        // only validate if it exists
        if (properties.contains(fullKey)) {
          validation.accept(s"$key.$i.")
        } else {
          // check if it is required
          if (requiredKeys.contains(subKey)) {
            throw new ValidationException(s"Required property key '$fullKey' is missing.")
          }
        }
      }
    }
  }

  /**
    * Validation for fixed indexed properties.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG, schema.fields.1.name = test2
    *
    * The propertyKeys map must define e.g. "type" and "name" and a validation logic for the
    * given full key.
    */
  def validateFixedIndexedProperties(
      key: String,
      allowEmpty: Boolean,
      propertyKeys: JMap[String, Consumer[String]])
    : Unit = {

    validateVariableIndexedProperties(
      key,
      allowEmpty,
      propertyKeys,
      new util.ArrayList(propertyKeys.keySet()))
  }

  /**
    * Validates a table schema property.
    */
  def validateTableSchema(key: String, isOptional: Boolean): Unit = {
    val nameValidation = (prefix: String) => {
      validateString(prefix + NAME, isOptional = false, minLen = 1)
    }
    val typeValidation = (prefix: String) => {
      validateType(prefix + TYPE, requireRow = false, isOptional = false)
    }

    validateFixedIndexedProperties(
      key,
      isOptional,
      Map(
        NAME -> toJava(nameValidation),
        TYPE -> toJava(typeValidation)
      ).asJava
    )
  }

  /**
    * Validates a enum property with a set of validation logic for each enum value.
    */
  def validateEnum(
      key: String,
      isOptional: Boolean,
      enumToValidation: JMap[String, Consumer[String]])
    : Unit = {

    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      val value = properties(key)
      if (!enumToValidation.containsKey(value)) {
        throw new ValidationException(s"Unknown value for property '$key'. " +
          s"Supported values [${enumToValidation.keySet().asScala.mkString(", ")}] but was: $value")
      } else {
        // run validation logic
        enumToValidation.get(value).accept(key)
      }
    }
  }

  /**
    * Validates a enum property with a set of enum values.
    */
  def validateEnumValues(key: String, isOptional: Boolean, values: JList[String]): Unit = {
    validateEnum(key, isOptional, values.asScala.map((_, noValidation())).toMap.asJava)
  }

  /**
    * Validates a type property.
    */
  def validateType(key: String, requireRow: Boolean, isOptional: Boolean): Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      // we don't validate the string but let the parser do the work for us
      // it throws a validation exception
      val info = TypeStringUtils.readTypeInfo(properties(key))
      if (requireRow && !info.isInstanceOf[RowTypeInfo]) {
        throw new ValidationException(
          s"Row type information expected for '$key' but was: ${properties(key)}")
      }
    }
  }

  /**
    * Validates that the given prefix is not included in these properties.
    */
  def validatePrefixExclusion(prefix: String): Unit = {
    val invalidField = properties.find(_._1.startsWith(prefix))
    if (invalidField.isDefined) {
      throw new ValidationException(
        s"Property '${invalidField.get._1}' is not allowed in this context.")
    }
  }

  /**
    * Validates that the given key is not included in these properties.
    */
  def validateExclusion(key: String): Unit = {
    if (properties.contains(key)) {
      throw new ValidationException(s"Property '$key' is not allowed in this context.")
    }
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns if any property contains parts of a given string.
    */
  def containsString(str: String): Boolean = {
    properties.exists(e => e._1.contains(str))
  }

  /**
    * Returns if the given key is contained.
    */
  def containsKey(key: String): Boolean = {
    properties.contains(key)
  }

  /**
    * Returns if a given prefix exists in the properties.
    */
  def hasPrefix(prefix: String): Boolean = {
    properties.exists(e => e._1.startsWith(prefix))
  }

  /**
    * Returns a Scala Map.
    */
  def asMap: JMap[String, String] = {
    properties.toMap.asJava
  }

  override def toString: String = {
    DescriptorProperties.toString(properties.toMap)
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns an empty validation logic.
    */
  def noValidation(): Consumer[String] = DescriptorProperties.emptyConsumer

  def exceptionSupplier(key: String): Supplier[TableException] = new Supplier[TableException] {
    override def get(): TableException = {
      new TableException(s"Property with key '$key' could not be found. " +
        s"This is a bug because the validation logic should have checked that before.")
    }
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Adds a property.
    */
  private def put(key: String, value: String): Unit = {
    if (properties.contains(key)) {
      throw new IllegalStateException("Property already present:" + key)
    }
    if (normalizeKeys) {
      properties.put(key.toLowerCase, value)
    } else {
      properties.put(key, value)
    }
  }

  /**
    * Gets an existing property.
    */
  private def get(key: String): String = {
    properties.getOrElse(
      key,
      throw exceptionSupplier(key).get())
  }

  /**
    * Raw access to the underlying properties map for testing purposes.
    */
  private[flink] def unsafePut(key: String, value: String): Unit = {
    properties.put(key, value)
  }

  /**
    * Raw access to the underlying properties map for testing purposes.
    */
  private[flink] def unsafeRemove(key: String): Unit = {
    properties.remove(key)
  }

  /**
    *  Adds a table schema under the given key.
    */
  private def putTableSchema(key: String, nameAndType: Seq[(String, String)]): Unit = {
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
  private def putIndexedFixedProperties(
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
  private def putIndexedVariableProperties(
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

  /**
    * Validates a property by first parsing the string value to a comparable object.
    * The boundaries are inclusive.
    */
  private def validateComparable[T <: Comparable[T]](
      key: String,
      isOptional: Boolean,
      min: T,
      max: T,
      parseFunction: String => T)
    : Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      val typeName = min.getClass.getSimpleName
      try {
        val value = parseFunction(properties(key))

        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
          throw new ValidationException(s"Property '$key' must be a $typeName" +
            s" value between $min and $max but was: ${properties(key)}")
        }
      } catch {
        case _: NumberFormatException =>
          throw new ValidationException(
            s"Property '$key' must be a $typeName value but was: ${properties(key)}")
      }
    }
  }
}

object DescriptorProperties {

  private val emptyConsumer: Consumer[String] = new Consumer[String] {
    override def accept(t: String): Unit = {
      // nothing to do
    }
  }

  val TYPE: String = "type"
  val NAME: String = "name"

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

  def deserialize[T](data: String, expected: Class[T]): T = {
    try {
      val byteData = Base64.decodeBase64(data)
      val obj = InstantiationUtil.deserializeObject[T](
        byteData,
        Thread.currentThread.getContextClassLoader)
      if (!expected.isAssignableFrom(obj.getClass)) {
        throw new ValidationException(
          s"Serialized data contains an object of unexpected type. " +
          s"Expected '${expected.getName}' but was '${obj.getClass.getName}'")
      }
      obj
    } catch {
      case e: Exception =>
        throw new ValidationException(s"Could not deserialize data: '$data'", e)
    }
  }

  def toString(keyOrValue: String): String = {
    StringEscapeUtils.escapeJava(keyOrValue).replace("\\/", "/") // '/' must not be escaped
  }

  def toString(key: String, value: String): String = {
    toString(key) + "=" + toString(value)
  }

  def toString(kv: Map[String, String]): String = {
    kv.map(e => DescriptorProperties.toString(e._1, e._2))
      .toSeq
      .sorted
      .mkString("\n")
  }

  def toJavaMap(descriptor: Descriptor): util.Map[String, String] = {
    val descriptorProperties = new DescriptorProperties()
    descriptor.addProperties(descriptorProperties)
    descriptorProperties.asMap
  }

  // the following methods help for Scala <-> Java interfaces
  // most of these methods are not necessary once we upgraded to Scala 2.12

  def toJava[T](option: Option[T]): Optional[T] = option match {
    case Some(v) => Optional.of(v)
    case None => Optional.empty()
  }

  def toScala[T](option: Optional[T]): Option[T] = Option(option.orElse(null.asInstanceOf[T]))

  def toJava[T](func: Function[T, Unit]): Consumer[T] = new Consumer[T] {
    override def accept(t: T): Unit = {
      func.apply(t)
    }
  }

  def toJava[T0, T1](tuple: (T0, T1)): JTuple2[T0, T1] = {
    new JTuple2[T0, T1](tuple._1, tuple._2)
  }
}
