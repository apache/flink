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

package org.apache.flink.modelserving.scala.server

import org.apache.flink.core.testutils.CommonTestUtils
import org.apache.flink.modelserving.scala.server.typeschema.ByteArraySchema

import org.junit.Assert.{assertArrayEquals, assertEquals}
import org.junit.Test

/**
  * Tests for the {@link SimpleStringSchema}.
  */
class ByteArraySchemaTest {

  @Test
  def testSerializationDesirailization(): Unit = {
    val bytes =  "hello world".getBytes

    assertArrayEquals(bytes, new ByteArraySchema().serialize(bytes))
    assertEquals(bytes, new ByteArraySchema().deserialize(bytes))
  }

  @Test
  def testSerializability(): Unit = {
    val schema = new ByteArraySchema
    val copy = CommonTestUtils.createCopySerializable(schema)
    assertEquals(schema.getProducedType, copy.getProducedType)
  }
}
