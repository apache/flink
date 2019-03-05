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

package org.apache.flink.table.calcite

import org.apache.flink.table.`type`.{InternalType, InternalTypes}

import org.junit.{Assert, Test}

class FlinkTypeFactoryTest {

  @Test
  def testInternalToRelType(): Unit = {
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

    def test(t: InternalType): Unit = {
      Assert.assertEquals(
        t,
        FlinkTypeFactory.toInternalType(
          typeFactory.createTypeFromInternalType(t, isNullable = true))
      )

      Assert.assertEquals(
        t,
        FlinkTypeFactory.toInternalType(
          typeFactory.createTypeFromInternalType(t, isNullable = false))
      )

      // twice for cache.
      Assert.assertEquals(
        t,
        FlinkTypeFactory.toInternalType(
          typeFactory.createTypeFromInternalType(t, isNullable = true))
      )

      Assert.assertEquals(
        t,
        FlinkTypeFactory.toInternalType(
          typeFactory.createTypeFromInternalType(t, isNullable = false))
      )
    }

    test(InternalTypes.BOOLEAN)
    test(InternalTypes.BYTE)
    test(InternalTypes.STRING)
    test(InternalTypes.DOUBLE)
    test(InternalTypes.FLOAT)
    test(InternalTypes.INT)
    test(InternalTypes.LONG)
    test(InternalTypes.SHORT)
    test(InternalTypes.BINARY)
    test(InternalTypes.DATE)
    test(InternalTypes.TIME)
    test(InternalTypes.TIMESTAMP)

    test(InternalTypes.createArrayType(InternalTypes.DOUBLE))
    test(InternalTypes.createMapType(InternalTypes.DOUBLE, InternalTypes.STRING))
    test(InternalTypes.createRowType(InternalTypes.DOUBLE, InternalTypes.STRING))
  }
}
