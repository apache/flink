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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.util.TestLogger
import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuiteLike

class EnumValueSerializerTest extends TestLogger with JUnitSuiteLike {

  /**
    * Tests that the snapshot configuration can be created and that the serializer
    * is compatible when being called with the created serializer snapshot
    */
  @Test
  def testEnumValueSerializerEnsureCompatibilityIdempotency() {
    val enumSerializer = new EnumValueSerializer(Letters)

    val snapshot = enumSerializer.snapshotConfiguration()

    assertTrue(snapshot.resolveSchemaCompatibility(enumSerializer).isCompatibleAsIs)
  }
}

@SerialVersionUID(-3883456191213905962L)
object Letters extends Enumeration {
  val A, B, C = Value
}
