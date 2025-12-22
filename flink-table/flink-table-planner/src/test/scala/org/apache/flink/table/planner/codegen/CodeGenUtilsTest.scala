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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.types.logical._

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream

import scala.collection.mutable.ArrayBuffer

class CodeGenUtilsTest {

  @Test
  def testNewName(): Unit = {
    val classLoader = Thread.currentThread().getContextClassLoader
    // Use name counter in CodeGenUtils.
    assertEquals("name$i0", CodeGenUtils.newName(null, "name"))
    assertEquals("name$i1", CodeGenUtils.newName(null, "name"))
    assertEquals(ArrayBuffer("name$i2", "id$i2"), CodeGenUtils.newNames(null, "name", "id"))

    val context1 = new CodeGeneratorContext(new Configuration, classLoader)
    // Use name counter in context1, the index will start from zero.
    assertEquals("name$0", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context1, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context1, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context1, "name", "id"))

    val context2 = new CodeGeneratorContext(new Configuration, classLoader)
    // Use name counter in context2, the index will start from zero.
    assertEquals("name$0", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context2, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context2, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context2, "name", "id"))

    val context3 = new CodeGeneratorContext(new Configuration, classLoader, context1)
    val context4 = new CodeGeneratorContext(new Configuration, classLoader, context3)
    // Use context4 to get a new name, the ancestor of context4(which is context1) will be used.
    assertEquals("name$5", CodeGenUtils.newName(context4, "name"))
    assertEquals("name$6", CodeGenUtils.newName(context4, "name"))
    assertEquals(ArrayBuffer("name$7", "id$7"), CodeGenUtils.newNames(context4, "name", "id"))
    assertEquals(ArrayBuffer("name$8", "id$8"), CodeGenUtils.newNames(context4, "name", "id"))
  }

  @ParameterizedTest
  @MethodSource(Array("basicTypesTestData"))
  def testPrimitiveDefaultValueForBasicTypes(
      logicalType: LogicalType,
      expectedDefault: String): Unit = {
    assertEquals(expectedDefault, CodeGenUtils.primitiveDefaultValue(logicalType))
  }

  @ParameterizedTest
  @MethodSource(Array("distinctTypeTestData"))
  def testPrimitiveDefaultValueForDistinctType(
      distinctType: DistinctType,
      expectedDefault: String): Unit = {
    assertEquals(expectedDefault, CodeGenUtils.primitiveDefaultValue(distinctType))
  }

}

object CodeGenUtilsTest {

  @MethodSource
  def basicTypesTestData(): stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // Basic primitive types
      Arguments.of(new BooleanType(), "false"),
      Arguments.of(new TinyIntType(), "((byte) -1)"),
      Arguments.of(new SmallIntType(), "((short) -1)"),
      Arguments.of(new IntType(), "-1"),
      Arguments.of(new BigIntType(), "-1L"),
      Arguments.of(new FloatType(), "-1.0f"),
      Arguments.of(new DoubleType(), "-1.0d"),

      // Date/time types that map to int/long
      Arguments.of(new DateType(), "-1"),
      Arguments.of(new TimeType(), "-1"),
      Arguments.of(new LocalZonedTimestampType(3), "null"),
      Arguments.of(new TimestampType(3), "null"),

      // Interval types
      Arguments.of(
        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
        "-1"),
      Arguments.of(
        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
        "-1L"),

      // String types
      Arguments.of(new VarCharType(100), s"${CodeGenUtils.BINARY_STRING}.EMPTY_UTF8"),
      Arguments.of(new CharType(10), s"${CodeGenUtils.BINARY_STRING}.EMPTY_UTF8"),

      // Complex types that should return "null"
      Arguments.of(new ArrayType(new IntType()), "null"),
      Arguments.of(new MapType(new IntType(), new VarCharType()), "null"),
      Arguments.of(RowType.of(new IntType(), new VarCharType()), "null"),
      Arguments.of(new DecimalType(10, 2), "null"),
      Arguments.of(new BinaryType(10), "null"),
      Arguments.of(new VarBinaryType(100), "null"),
      Arguments.of(new RawType(classOf[Void], VoidSerializer.INSTANCE), "null")
    )
  }

  @MethodSource
  def distinctTypeTestData(): stream.Stream[Arguments] = {
    val objectIdentifier = ObjectIdentifier.of("catalog", "database", "distinct_type")
    java.util.stream.Stream.of(
      // Distinct types based on basic types
      Arguments.of(
        DistinctType
          .newBuilder(objectIdentifier, new IntType())
          .build(),
        "-1"),
      Arguments.of(
        DistinctType
          .newBuilder(objectIdentifier, new SmallIntType())
          .build(),
        "((short) -1)"),
      Arguments.of(
        DistinctType
          .newBuilder(objectIdentifier, new TinyIntType())
          .build(),
        "((byte) -1)"),
      Arguments.of(
        DistinctType
          .newBuilder(objectIdentifier, new BigIntType())
          .build(),
        "-1L"),
      Arguments.of(
        DistinctType
          .newBuilder(objectIdentifier, new BooleanType())
          .build(),
        "false")
    )
  }
}
