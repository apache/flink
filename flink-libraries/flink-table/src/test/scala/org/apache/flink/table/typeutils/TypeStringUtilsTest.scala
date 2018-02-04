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

package org.apache.flink.table.typeutils

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.utils.CommonTestData.{NonPojo, Person}
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Tests for string-based representation of [[TypeInformation]].
  */
class TypeStringUtilsTest {

  @Test
  def testPrimitiveTypes(): Unit = {
    testReadAndWrite("VARCHAR", Types.STRING)
    testReadAndWrite("BOOLEAN", Types.BOOLEAN)
    testReadAndWrite("TINYINT", Types.BYTE)
    testReadAndWrite("SMALLINT", Types.SHORT)
    testReadAndWrite("INT", Types.INT)
    testReadAndWrite("BIGINT", Types.LONG)
    testReadAndWrite("FLOAT", Types.FLOAT)
    testReadAndWrite("DOUBLE", Types.DOUBLE)
    testReadAndWrite("DECIMAL", Types.DECIMAL)
    testReadAndWrite("DATE", Types.SQL_DATE)
    testReadAndWrite("TIME", Types.SQL_TIME)
    testReadAndWrite("TIMESTAMP", Types.SQL_TIMESTAMP)

    // unsupported type information
    testReadAndWrite(
      "ANY(java.lang.Void, " +
        "rO0ABXNyADJvcmcuYXBhY2hlLmZsaW5rLmFwaS5jb21tb24udHlwZWluZm8uQmFzaWNUeXBlSW5mb_oE8IKl" +
        "ad0GAgAETAAFY2xhenp0ABFMamF2YS9sYW5nL0NsYXNzO0wAD2NvbXBhcmF0b3JDbGFzc3EAfgABWwAXcG9z" +
        "c2libGVDYXN0VGFyZ2V0VHlwZXN0ABJbTGphdmEvbGFuZy9DbGFzcztMAApzZXJpYWxpemVydAA2TG9yZy9h" +
        "cGFjaGUvZmxpbmsvYXBpL2NvbW1vbi90eXBldXRpbHMvVHlwZVNlcmlhbGl6ZXI7eHIANG9yZy5hcGFjaGUu" +
        "ZmxpbmsuYXBpLmNvbW1vbi50eXBlaW5mby5UeXBlSW5mb3JtYXRpb26UjchIurN66wIAAHhwdnIADmphdmEu" +
        "bGFuZy5Wb2lkAAAAAAAAAAAAAAB4cHB1cgASW0xqYXZhLmxhbmcuQ2xhc3M7qxbXrsvNWpkCAAB4cAAAAABz" +
        "cgA5b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLlZvaWRTZXJpYWxpemVyAAAA" +
        "AAAAAAECAAB4cgBCb3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1dGlscy5iYXNlLlR5cGVTZXJp" +
        "YWxpemVyU2luZ2xldG9ueamHqscud0UCAAB4cgA0b3JnLmFwYWNoZS5mbGluay5hcGkuY29tbW9uLnR5cGV1" +
        "dGlscy5UeXBlU2VyaWFsaXplcgAAAAAAAAABAgAAeHA)",
      BasicTypeInfo.VOID_TYPE_INFO)
  }

  @Test
  def testWriteComplexTypes(): Unit = {
    testReadAndWrite(
      "ROW(f0 DECIMAL, f1 TINYINT)",
      Types.ROW(Types.DECIMAL, Types.BYTE))

    testReadAndWrite(
      "ROW(hello DECIMAL, world TINYINT)",
      Types.ROW(
        Array[String]("hello", "world"),
        Array[TypeInformation[_]](Types.DECIMAL, Types.BYTE)))

    testReadAndWrite(
      "ROW(\"he llo\" DECIMAL, world TINYINT)",
      Types.ROW(
        Array[String]("he llo", "world"),
        Array[TypeInformation[_]](Types.DECIMAL, Types.BYTE)))

    testReadAndWrite(
      "ROW(\"he         \\nllo\" DECIMAL, world TINYINT)",
      Types.ROW(
        Array[String]("he         \nllo", "world"),
        Array[TypeInformation[_]](Types.DECIMAL, Types.BYTE)))

    testReadAndWrite(
      "POJO(org.apache.flink.table.runtime.utils.CommonTestData$Person)",
      TypeExtractor.createTypeInfo(classOf[Person]))

    testReadAndWrite(
      "ANY(org.apache.flink.table.runtime.utils.CommonTestData$NonPojo)",
      TypeExtractor.createTypeInfo(classOf[NonPojo]))
  }

  private def testReadAndWrite(expected: String, tpe: TypeInformation[_]): Unit = {
    // test write to string
    assertEquals(expected, TypeStringUtils.writeTypeInfo(tpe))

    // test read from string
    assertEquals(tpe, TypeStringUtils.readTypeInfo(expected))
  }
}
