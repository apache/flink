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

import java.util

import org.apache.flink.table.api.{Types, ValidationException}
import org.junit.Test

import scala.collection.JavaConverters._

class SchemaTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidType(): Unit = {
    addPropertyAndVerify(
      descriptors().get(0),
      "schema.1.type", "dfghj")
  }

  @Test(expected = classOf[ValidationException])
  def testBothRowtimeAndProctime(): Unit = {
    addPropertyAndVerify(
      descriptors().get(0),
      "schema.2.rowtime.watermarks.type", "from-source")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc1 = new Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
      new Rowtime().timestampsFromSource().watermarksFromSource())

    val desc2 = new Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP)

    util.Arrays.asList(desc1, desc2)
  }

  override def validator(): DescriptorValidator = {
    new SchemaValidator(true, true, true)
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props1 = Map(
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
      "schema.3.rowtime.watermarks.type" -> "from-source",
      "schema.3.rowtime.timestamps.type" -> "from-source"
    )

    val props2 = Map(
      "schema.0.name" -> "myField",
      "schema.0.type" -> "BOOLEAN",
      "schema.1.name" -> "otherField",
      "schema.1.type" -> "VARCHAR",
      "schema.1.from" -> "csvField",
      "schema.2.name" -> "p",
      "schema.2.type" -> "TIMESTAMP",
      "schema.2.proctime" -> "true",
      "schema.3.name" -> "r",
      "schema.3.type" -> "TIMESTAMP"
    )

    util.Arrays.asList(props1.asJava, props2.asJava)
  }
}
