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

import org.apache.flink.table.api.ValidationException
import org.junit.Test

class JsonTest extends DescriptorTestBase {

  @Test
  def testJson(): Unit = {
    val schema =
      """
        |{
        |    "title": "Person",
        |    "type": "object",
        |    "properties": {
        |        "firstName": {
        |            "type": "string"
        |        },
        |        "lastName": {
        |            "type": "string"
        |        },
        |        "age": {
        |            "description": "Age in years",
        |            "type": "integer",
        |            "minimum": 0
        |        }
        |    },
        |    "required": ["firstName", "lastName"]
        |}
        |""".stripMargin
    val desc = Json()
      .schema(schema)
      .failOnMissingField(true)
    val expected = Seq(
      "format.type" -> "json",
      "format.version" -> "1",
      "format.schema-string" -> schema,
      "format.fail-on-missing-field" -> "true")
    verifyProperties(desc, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMissingField(): Unit = {
    verifyInvalidProperty("format.fail-on-missing-field", "DDD")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingSchema(): Unit = {
    verifyMissingProperty("format.schema-string")
  }

  override def descriptor(): Descriptor = {
    Json().schema("test")
  }

  override def validator(): DescriptorValidator = {
    new JsonValidator()
  }
}
