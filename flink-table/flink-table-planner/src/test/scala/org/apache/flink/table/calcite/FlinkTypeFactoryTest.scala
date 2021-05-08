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

import org.apache.flink.api.scala.typeutils.Types

import org.apache.calcite.sql.`type`.SqlTypeName
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

/** Test cases for [[FlinkTypeFactory]]. */
class FlinkTypeFactoryTest {

  @Test
  def testCanonizeType(): Unit = {
    val typeFactory = FlinkTypeFactory.INSTANCE
    val genericRelType = typeFactory
        .createTypeFromTypeInfo(Types.GENERIC(classOf[TestClass]), isNullable = true)
    val genericRelType2 = typeFactory
        .createTypeFromTypeInfo(Types.GENERIC(classOf[TestClass]), isNullable = true)
    val genericRelType3 = typeFactory
        .createTypeFromTypeInfo(Types.GENERIC(classOf[TestClass2]), isNullable = true)

    assertTrue("The type expect to be canonized", genericRelType eq genericRelType2)
    assertFalse("The type expect to be not canonized", genericRelType eq genericRelType3)
    assertFalse("The type expect to be not canonized",
      typeFactory.builder().add("f0", genericRelType).build()
          eq typeFactory.builder().add("f0", genericRelType3).build())

    val varchar20Type = typeFactory.createSqlType(SqlTypeName.VARCHAR, 20)
    val bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT)

    val arrayRelType = typeFactory.createArrayType(varchar20Type, 10)
    val arrayRelType2 = typeFactory.createArrayType(varchar20Type, 10)
    val arrayRelType3 = typeFactory.createArrayType(bigintType, 10)
    assertTrue("The type expect to be canonized", arrayRelType eq arrayRelType2)
    assertFalse("The type expect to be not canonized", arrayRelType eq arrayRelType3)
    assertFalse("The type expect to be not canonized",
      typeFactory.builder().add("f0", arrayRelType).build()
          eq typeFactory.builder().add("f0", arrayRelType3).build())

    val multisetRelType = typeFactory.createMultisetType(varchar20Type, 10)
    val multisetRelType2 = typeFactory.createMultisetType(varchar20Type, 10)
    val multisetRelType3 = typeFactory.createMultisetType(bigintType, 10)
    assertTrue("The type expect to be canonized", multisetRelType eq multisetRelType2)
    assertFalse("The type expect to be not canonized", multisetRelType eq multisetRelType3)
    assertFalse("The type expect to be not canonized",
      typeFactory.builder().add("f0", multisetRelType).build()
          eq typeFactory.builder().add("f0", multisetRelType3).build())
  }

  case class TestClass(f0: Int, f1: String)

  case class TestClass2(f0: Int, f1: String)
}
