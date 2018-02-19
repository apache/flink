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
import java.util.function.{Consumer, Supplier}
import java.util.regex.Pattern
import java.util.{Optional, List => JList, Map => JMap}

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringEscapeUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.{TableException, TableSchema, ValidationException}
import org.apache.flink.table.descriptors.DescriptorProperties.{NAME, TYPE, normalizeTableSchema}
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
  * E.g., conntector.schema.start-from = from-earliest. Try not to use the higher level in a
  * key-name. E.g., instead of connector.kafka.kafka-version use connector.kafka.version.
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

  /**
    * Adds a set of properties.
    */
  def putProperties(properties: Map[String, String]): Unit = {
    properties.foreach { case (k, v) =>
      put(k, v)
    }
  }

  /**
    * Adds a set of properties. This method is intended for Java code.
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
    putTableSchema(key, normalizeTableSchema(schema))
  }

  /**
    * Adds a table schema under the given key.
    */
  def putTableSchema(key: String, nameAndType: Seq[(String, String)]): Unit = {
    putIndexedFixedProperties(
      key,
      Seq(NAME, TYPE),
      nameAndType.map(t => Seq(t._1, t._2))
    )
  }

  /**
    * Adds a table schema under the given key. This method is intended for Java code.
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
    * Adds an indexed sequence of properties (with sub-properties) under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG, schema.fields.1.name = test2
    *
    * The arity of each propertyValue must match the arity of propertyKeys.
    *
    * This method is intended for Java code.
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
    * Adds an indexed mapping of properties under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    *                             schema.fields.1.name = test2
    *
    * The arity of the propertySets can differ.
    *
    * This method is intended for Java code.
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
  def getString(key: String): Option[String] = {
    properties.get(key)
  }

  /**
    * Returns a string value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalString(key: String): Optional[String] = toJava(getString(key))

  /**
    * Returns a character value under the given key if it exists.
    */
  def getCharacter(key: String): Option[Character] = getString(key).map { c =>
    if (c.length != 1) {
      throw new ValidationException(s"The value of $key must only contain one character.")
    }
    c.charAt(0)
  }

  /**
    * Returns a class value under the given key if it exists.
    */
  def getClass[T](key: String, superClass: Class[T]): Option[Class[T]] = {
    properties.get(key).map { name =>
      val clazz = try {
        Class.forName(
          name,
          true,
          Thread.currentThread().getContextClassLoader).asInstanceOf[Class[T]]
      } catch {
        case e: Exception =>
          throw new ValidationException(s"Coult not get class for key '$key'.", e)
      }
      if (!superClass.isAssignableFrom(clazz)) {
        throw new ValidationException(s"Class '$name' does not extend from the required " +
          s"class '${superClass.getName}' for key '$key'.")
      }
      clazz
    }
  }

  /**
    * Returns a character value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalCharacter(key: String): Optional[Character] = toJava(getCharacter(key))

  /**
    * Returns a boolean value under the given key if it exists.
    */
  def getBoolean(key: String): Option[Boolean] = getString(key).map(JBoolean.parseBoolean(_))

  /**
    * Returns a boolean value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalBoolean(key: String): Optional[java.lang.Boolean] =
    toJava(getBoolean(key).map(Boolean.box))

  /**
    * Returns an integer value under the given key if it exists.
    */
  def getInt(key: String): Option[Int] = getString(key).map(JInt.parseInt(_))

  /**
    * Returns an integer value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalInt(key: String): Optional[java.lang.Integer] = toJava(getInt(key).map(Int.box))

  /**
    * Returns a long value under the given key if it exists.
    */
  def getLong(key: String): Option[Long] = getString(key).map(JLong.parseLong(_))

  /**
    * Returns a long value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalLong(key: String): Optional[java.lang.Long] = toJava(getLong(key).map(Long.box))

  /**
    * Returns a double value under the given key if it exists.
    */
  def getDouble(key: String): Option[Double] = getString(key).map(JDouble.parseDouble(_))

  /**
    * Returns a long value under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalDouble(key: String): Optional[Double] = toJava(getDouble(key).map(Double.box))

  /**
    * Returns a table schema under the given key if it exists.
    */
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

  /**
    * Returns a table schema under the given key if it exists.
    */
  def getOptionalTableSchema(key: String): Optional[TableSchema] = toJava(getTableSchema(key))

  /**
    * Returns the type information under the given key if it exists.
    */
  def getType(key: String): Option[TypeInformation[_]] = {
    properties.get(key).map(TypeStringUtils.readTypeInfo)
  }

  /**
    * Returns the type information under the given key if it exists.
    * This method is intended for Java code.
    */
  def getOptionalType(key: String): Optional[TypeInformation[_]] = {
    toJava(getType(key))
  }

  /**
    * Returns a prefix subset of properties.
    */
  def getPrefix(prefixKey: String): Map[String, String] = {
    val prefix = prefixKey + '.'
    properties.filterKeys(_.startsWith(prefix)).toSeq.map{ case (k, v) =>
      k.substring(prefix.length) -> v // remove prefix
    }.toMap
  }

  /**
    * Returns a prefix subset of properties.
    * This method is intended for Java code.
    */
  def getPrefixMap(prefixKey: String): JMap[String, String] = getPrefix(prefixKey).asJava

  /**
    * Returns all properties under a given key that contains an index in between.
    *
    * E.g. rowtime.0.name -> returns all rowtime.#.name properties
    */
  def getIndexedProperty(key: String, property: String): Map[String, String] = {
    val escapedKey = Pattern.quote(key)
    properties.filterKeys(k => k.matches(s"$escapedKey\\.\\d+\\.$property")).toMap
  }

  /**
    * Returns all properties under a given key that contains an index in between.
    *
    * E.g. rowtime.0.name -> returns all rowtime.#.name properties
    *
    * This method is intended for Java code.
    */
  def getIndexedPropertyMap(key: String, property: String): JMap[String, String] =
    getIndexedProperty(key, property).asJava

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
    * Validates an integer property. The boundaries are inclusive.
    */
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

  /**
    * Validates a long property. The boundaries are inclusive.
    */
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
    * Validates a double property. The boundaries are inclusive.
    */
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

  /**
    * Validates a table schema property.
    */
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

  /**
    * Validates a enum property with a set of validation logic for each enum value.
    */
  def validateEnum(
      key: String,
      isOptional: Boolean,
      enumToValidation: Map[String, (DescriptorProperties) => Unit])
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
        enumToValidation(value).apply(this) // run validation logic
      }
    }
  }

  /**
    * Validates a enum property with a set of validation logic for each enum value.
    * This method is intended for Java code.
    */
  def validateEnum(
      key: String,
      isOptional: Boolean,
      enumToValidation: JMap[String, Consumer[DescriptorProperties]])
    : Unit = {
    val converted = enumToValidation.asScala.mapValues { c =>
      (props: DescriptorProperties) => c.accept(props)
    }.toMap
    validateEnum(key, isOptional, converted)
  }

  /**
    * Validates a enum property with a set of enum values.
    */
  def validateEnumValues(key: String, isOptional: Boolean, values: Seq[String]): Unit = {
    validateEnum(key, isOptional, values.map(_ -> DescriptorProperties.emptyFunction).toMap)
  }

  /**
    * Validates a enum property with a set of enum values.
    * This method is intended for Java code.
    */
  def validateEnumValues(key: String, isOptional: Boolean, values: JList[String]): Unit = {
    validateEnum(key, isOptional, values.asScala.map(_ -> DescriptorProperties.emptyFunction).toMap)
  }

  /**
    * Validates a type property.
    */
  def validateType(key: String, isOptional: Boolean): Unit = {
    if (!properties.contains(key)) {
      if (!isOptional) {
        throw new ValidationException(s"Could not find required property '$key'.")
      }
    } else {
      TypeStringUtils.readTypeInfo(properties(key)) // throws validation exceptions
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
  def asScalaMap: Map[String, String] = {
    properties.toMap
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns an empty validation logic.
    */
  def noValidation(): DescriptorProperties => Unit = DescriptorProperties.emptyFunction

  /**
    * Returns an empty validation logic.
    * This method is intended for Java code.
    */
  def noValidationConsumer(): Consumer[DescriptorProperties] = DescriptorProperties.emptyConsumer

  /**
    * Returns a throwable supplier.
    */
  def errorSupplier(): Supplier[TableException] = DescriptorProperties.errorSupplier

  // ----------------------------------------------------------------------------------------------

  private def toJava[T](option: Option[T]): Optional[T] = option match {
    case Some(v) => Optional.of(v)
    case None => Optional.empty()
  }
}

object DescriptorProperties {

  private val emptyFunction: DescriptorProperties => Unit = (_: DescriptorProperties) => {}
  // Scala compiler complains otherwise
  private val emptyConsumer: Consumer[DescriptorProperties] = new Consumer[DescriptorProperties] {
    override def accept(t: DescriptorProperties): Unit = {}
  }
  private val errorSupplier: Supplier[TableException] = new Supplier[TableException] {
    override def get(): TableException = {
      new TableException("This value should be present. " +
        "This indicates a bug in the validation logic. Please file an issue.")
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
    StringEscapeUtils.escapeJava(keyOrValue)
  }

  def toString(key: String, value: String): String = {
    toString(key) + "=" + toString(value)
  }
}
