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

package org.apache.flink.api.table.typeinfo

import org.apache.commons.lang3.SerializationUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestOutputView
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.table.Row
import org.junit.Assert._
import org.junit.Test

class RowSerializerTest {

  private val rowInfo: TypeInformation[Row] = new RowTypeInfo(
    Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), Seq("id", "name"))

  private val row1 = new Row(2)
  row1.setField(0, 1)
  row1.setField(1, "a")

  private val row2 = new Row(2)
  row2.setField(0, 2)
  row2.setField(1, "hello")

  private val testData: Array[Row] = Array(row1, row2)

  private val rowSerializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)

  @Test
  def testInstantiate() = {
    val serializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)
    val instance: Row = serializer.createInstance()
    assertNotNull("The created instance must not be null.", instance)
    val `type`: Class[Row] = classOf[Row]
    assertNotNull("The test is corrupt: type class is null.", `type`)
    assertEquals("Type of the instantiated object is wrong.", `type`, instance.getClass)
  }

  @Test
  def testGetLength() = {
    assertEquals(-1, rowSerializer.getLength)
  }

  def compareRows(original: Row, copy: Row, msg: String) = {
    (0 to original.productArity - 1).foreach {
      index =>
        val copiedValue: Any = copy.productElement(index)
        val element: Any = original.productElement(index)
        assertEquals(msg, element, copiedValue)
    }
  }

  @Test
  def testCopy() = {
    testData.foreach {
      row =>
        val copy: Row = rowSerializer.copy(row)
        compareRows(row, copy, "Copied element is not equal to the original element.")
    }
  }

  @Test
  def testCopyIntoNewElements() = {
    testData.foreach {
      row =>
        val copy: Row = rowSerializer.copy(row, rowSerializer.createInstance())
        compareRows(row, copy, "Copied element is not equal to the original element.")
    }
  }

  @Test
  def testCopyIntoReusedElements() = {
    val target: Row = rowSerializer.createInstance
    for (datum <- testData) {
      val copy: Row = rowSerializer.copy(datum, target)
      compareRows(datum, copy, "Copied element is not equal to the original element.")
      assertEquals(target, copy)
    }
  }

  @Test
  def testSerializeIndividually() = {
    testData.foreach {
      value =>
        val out = new TestOutputView()
        rowSerializer.serialize(value, out)
        val in = out.getInputView

        assertTrue("No data available during deserialization.", in.available() > 0)

        val deserialized = rowSerializer.deserialize(rowSerializer.createInstance(), in)
        compareRows(value, deserialized, "Deserialized value is wrong.")

        assertTrue("Trailing data available after deserialization.", in.available() == 0)
    }
  }

  @Test
  def testSerializeIndividuallyReusingValues() = {
    testData.foreach {
      value =>
        val reuseValue: Row = rowSerializer.createInstance
        val out = new TestOutputView()
        rowSerializer.serialize(value, out)
        val in = out.getInputView

        assertTrue("No data available during deserialization.", in.available() > 0)

        val deserialized = rowSerializer.deserialize(reuseValue, in)
        compareRows(value, deserialized, "Deserialized value is wrong.")

        assertTrue("Trailing data available after deserialization.", in.available() == 0)
        assertEquals(reuseValue, deserialized)
    }
  }

  @Test
  def testSerializeAsSequenceNoReuse() = {

    val out = new TestOutputView()
    testData.foreach {
      value =>
        rowSerializer.serialize(value, out)
    }

    val in = out.getInputView

    var num = 0
    while (in.available() > 0) {
      val deserialized = rowSerializer.deserialize(in)
      deserialized.toString()

      compareRows(testData(num), deserialized, "Deserialized value is wrong")
      num += 1
    }

    assertEquals("Wrong number of elements deserialized.", testData.length, num)
  }

  @Test
  def testSerializeAsSequenceReusingValues() = {

    val out = new TestOutputView()
    testData.foreach {
      value =>
        rowSerializer.serialize(value, out)
    }

    val in = out.getInputView
    val reuseValue = rowSerializer.createInstance()

    var num = 0
    while (in.available() > 0) {
      val deserialized = rowSerializer.deserialize(reuseValue, in)
      deserialized.toString()

      compareRows(testData(num), deserialized, "Deserialized value is wrong")
      assertEquals(deserialized, reuseValue)
      num += 1
    }

    assertEquals("Wrong number of elements deserialized.", testData.length, num)
  }

  @Test
  def testSerializedCopyIndividually() = {

    testData.foreach {
      value =>
        val out = new TestOutputView()
        rowSerializer.serialize(value, out)

        val source = out.getInputView

        val target = new TestOutputView()
        rowSerializer.copy(source, target)
        val toVerify = target.getInputView

        assertTrue("No data available copying.", toVerify.available() > 0)

        val deserialized = rowSerializer.deserialize(rowSerializer.createInstance(), toVerify)

        compareRows(value, deserialized, "Deserialized value is wrong")

        assertTrue("Trailing data available after deserialization.", toVerify.available() == 0);
    }
  }

  @Test
  def testSerializedCopyAsSequence() = {
    val out = new TestOutputView()
    testData.foreach {
      value =>
        rowSerializer.serialize(value, out);
    }

    val source = out.getInputView
    val target = new TestOutputView()
    testData.indices.foreach {
      entry =>
        rowSerializer.copy(source, target);
    }

    val toVerify = target.getInputView
    var num = 0

    while (toVerify.available() > 0) {
      val deserialized = rowSerializer.deserialize(rowSerializer.createInstance(), toVerify)

      compareRows(testData(num), deserialized, "Deserialized value is wrong")
      num += 1
    }

    assertEquals("Wrong number of elements copied.", testData.length, num)
  }

  @Test
  def testSerializabilityAndEquals() {
    val ser2 = SerializationUtils.clone(rowSerializer)
    assertEquals("Copy of the serializer is not equal to the original one.", rowSerializer, ser2)
  }


}
