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
import java.util.Collections

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.util.JavaScalaConversionUtil.toJava
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Tests for [[DescriptorProperties]].
  */
class DescriptorPropertiesTest {

  private val ARRAY_KEY = "my-array"

  @Test
  def testEquals(): Unit = {
    val properties1 = new DescriptorProperties()
    properties1.putString("hello1", "12")
    properties1.putString("hello2", "13")
    properties1.putString("hello3", "14")

    val properties2 = new DescriptorProperties()
    properties2.putString("hello1", "12")
    properties2.putString("hello2", "13")
    properties2.putString("hello3", "14")

    val properties3 = new DescriptorProperties()
    properties3.putString("hello1", "12")
    properties3.putString("hello3", "14")
    properties3.putString("hello2", "13")

    assertEquals(properties1, properties2)

    assertEquals(properties1, properties3)
  }

  @Test
  def testMissingArray(): Unit = {
    val properties = new DescriptorProperties()

    testArrayValidation(properties, 0, Integer.MAX_VALUE)
  }

  @Test
  def testArrayValues(): Unit = {
    val properties = new DescriptorProperties()

    properties.putString(s"$ARRAY_KEY.0", "12")
    properties.putString(s"$ARRAY_KEY.1", "42")
    properties.putString(s"$ARRAY_KEY.2", "66")

    testArrayValidation(properties, 1, Integer.MAX_VALUE)

    assertEquals(
      util.Arrays.asList(12, 42, 66),
      properties.getArray(ARRAY_KEY, toJava((key: String) => {
        properties.getInt(key)
      })))
  }

  @Test
  def testArraySingleValue(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString(ARRAY_KEY, "12")

    testArrayValidation(properties, 1, Integer.MAX_VALUE)

    assertEquals(
      Collections.singletonList(12),
      properties.getArray(ARRAY_KEY, toJava((key: String) => {
        properties.getInt(key)
      })))
  }

  @Test(expected = classOf[ValidationException])
  def testArrayInvalidValues(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString(s"$ARRAY_KEY.0", "12")
    properties.putString(s"$ARRAY_KEY.1", "INVALID")
    properties.putString(s"$ARRAY_KEY.2", "66")

    testArrayValidation(properties, 1, Integer.MAX_VALUE)
  }

  @Test(expected = classOf[ValidationException])
  def testArrayInvalidSingleValue(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString(ARRAY_KEY, "INVALID")

    testArrayValidation(properties, 1, Integer.MAX_VALUE)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMissingArray(): Unit = {
    val properties = new DescriptorProperties()

    testArrayValidation(properties, 1, Integer.MAX_VALUE)
  }

  @Test
  def testRemoveKeys(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("hello1", "12")
    properties.putString("hello2", "13")
    properties.putString("hello3", "14")

    val actual = properties.withoutKeys(util.Arrays.asList("hello1", "hello3"))

    val expected = new DescriptorProperties()
    expected.putString("hello2", "13")

    assertEquals(expected, actual)
  }

  @Test
  def testPrefixedMap(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("hello1", "12")
    properties.putString("hello2", "13")
    properties.putString("hello3", "14")

    val actual = properties.asPrefixedMap("prefix.")

    val expected = new DescriptorProperties()
    expected.putString("prefix.hello1", "12")
    expected.putString("prefix.hello2", "13")
    expected.putString("prefix.hello3", "14")

    assertEquals(expected.asMap, actual)
  }

  private def testArrayValidation(
      properties: DescriptorProperties,
      minLength: Int,
      maxLength: Int)
    : Unit = {
    val validator: (String) => Unit = (key: String) => {
      properties.validateInt(key, false)
    }

    properties.validateArray(
      ARRAY_KEY,
      toJava(validator),
      minLength,
      maxLength)
  }
}
