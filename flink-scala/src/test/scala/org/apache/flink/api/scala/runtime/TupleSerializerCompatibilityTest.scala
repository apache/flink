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

package org.apache.flink.api.scala.runtime

import java.io.InputStream

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSerializationUtil, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerConfigSnapshot
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.runtime.TupleSerializerCompatibilityTestGenerator._
import org.apache.flink.api.scala.typeutils.CaseClassSerializer
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.Test

/**
  * Test for ensuring backwards compatibility of tuples and case classes across Scala versions.
  */
class TupleSerializerCompatibilityTest {

  @Test
  def testCompatibilityWithFlink_1_3(): Unit = {
    var is: InputStream = null
    try {
      is = getClass.getClassLoader.getResourceAsStream(SNAPSHOT_RESOURCE)
      val snapshotIn = new DataInputViewStreamWrapper(is)

      val deserialized = TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
        snapshotIn,
        getClass.getClassLoader)

      assertEquals(1, deserialized.size)

      val oldSerializer: TypeSerializer[TestCaseClass] =
        deserialized.get(0).f0.asInstanceOf[TypeSerializer[TestCaseClass]]

      val oldConfigSnapshot: TypeSerializerSnapshot[TestCaseClass] =
        deserialized.get(0).f1.asInstanceOf[TypeSerializerSnapshot[TestCaseClass]]

      // test serializer and config snapshot
      assertNotNull(oldSerializer)
      assertNotNull(oldConfigSnapshot)
      assertTrue(oldSerializer.isInstanceOf[CaseClassSerializer[_]])
      assertTrue(oldConfigSnapshot.isInstanceOf[TupleSerializerConfigSnapshot[_]])

      assertTrue(oldConfigSnapshot.isInstanceOf[TupleSerializerConfigSnapshot[_]])

      val currentSerializer = createTypeInformation[TestCaseClass]
        .createSerializer(new ExecutionConfig())
      assertTrue(oldConfigSnapshot
        .resolveSchemaCompatibility(currentSerializer)
        .isCompatibleAsIs)

      // test old data serialization
      is.close()
      is = getClass.getClassLoader.getResourceAsStream(DATA_RESOURCE)
      var dataIn = new DataInputViewStreamWrapper(is)

      assertEquals(TEST_DATA_1, oldSerializer.deserialize(dataIn))
      assertEquals(TEST_DATA_2, oldSerializer.deserialize(dataIn))
      assertEquals(TEST_DATA_3, oldSerializer.deserialize(dataIn))

      // test new data serialization
      is.close()
      is = getClass.getClassLoader.getResourceAsStream(DATA_RESOURCE)
      dataIn = new DataInputViewStreamWrapper(is)
      assertEquals(TEST_DATA_1, currentSerializer.deserialize(dataIn))
      assertEquals(TEST_DATA_2, currentSerializer.deserialize(dataIn))
      assertEquals(TEST_DATA_3, currentSerializer.deserialize(dataIn))
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }
}
