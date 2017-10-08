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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.Test

class HashFunctionTest extends ExpressionTestBase {

  @Test
  def testCalcHash(): Unit = {
    val expectedMd5 = "098f6bcd4621d373cade4e832627b4f6"
    val expectedSha1 = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
    val expectedSha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

    testAllApis(
      md5("test"),
      "md5(\"test\")",
      "MD5('test')",
      expectedMd5)

    testAllApis(
      sha1("test"),
      "sha1(\"test\")",
      "SHA1('test')",
      expectedSha1)

    testAllApis(
      sha256("test"),
      "sha256(\"test\")",
      "SHA256('test')",
      expectedSha256)

    testAllApis(
      md5('f2),
      "sha256(f2)",
      "SHA256(f2)",
      "null")

    testAllApis(
      sha1('f2),
      "sha256(f2)",
      "SHA256(f2)",
      "null")

    testAllApis(
      sha256('f2),
      "sha256(f2)",
      "SHA256(f2)",
      "null")
  }

  override def testData: Any = {
    val testData = new Row(3)
    testData.setField(0, "")
    testData.setField(1, "test")
    testData.setField(2, null)
    testData
  }

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo(
      Types.STRING,
      Types.STRING,
      Types.STRING).asInstanceOf[TypeInformation[Any]]
}
