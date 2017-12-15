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

import org.apache.flink.table.api.{Types, ValidationException}
import org.junit.Test

class SchemaTest extends DescriptorTestBase {

  @Test
  def testSchema(): Unit = {
    val desc = Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())
    val expected = Seq(
      "schema.version" -> "1",
      "schema.0.name" -> "myField",
      "schema.0.type" -> "BOOLEAN",
      "schema.1.name" -> "otherField",
      "schema.1.type" -> "VARCHAR",
      "schema.1.from" -> "csvField",
      "schema.2.name" -> "p",
      "schema.2.type" -> "TIMESTAMP",
      "schema.2.proctime" -> "true",
      "schema.3.name" -> "r",
      "schema.3.type" -> "TIMESTAMP",
      "schema.3.rowtime.0.version" -> "1",
      "schema.3.rowtime.0.watermarks.type" -> "from-source",
      "schema.3.rowtime.0.timestamps.type" -> "from-source"
    )
    verifyProperties(desc, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidType(): Unit = {
    verifyInvalidProperty("schema.1.type", "dfghj")
  }

  @Test(expected = classOf[ValidationException])
  def testBothRowtimeAndProctime(): Unit = {
    verifyInvalidProperty("schema.2.rowtime.0.version", "1")
    verifyInvalidProperty("schema.2.rowtime.0.watermarks.type", "from-source")
    verifyInvalidProperty("schema.2.rowtime.0.timestamps.type", "from-source")
  }

  override def descriptor(): Descriptor = {
    Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP)
  }

  override def validator(): DescriptorValidator = {
    new SchemaValidator(isStreamEnvironment = true)
  }
}
