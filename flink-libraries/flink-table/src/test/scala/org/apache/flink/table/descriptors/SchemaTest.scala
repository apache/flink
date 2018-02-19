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
    verifyInvalidProperty(
      descriptors().get(0),
      "schema.fields.1.type", "dfghj")
  }

  @Test(expected = classOf[ValidationException])
  def testBothRowtimeAndProctime(): Unit = {
    verifyInvalidProperty(
      descriptors().get(0),
      "schema.fields.2.rowtime.watermarks.type", "from-source")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc1 = Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())

    val desc2 = Schema()
      .field("myField", Types.BOOLEAN)
      .field("otherField", "VARCHAR").from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP)

    val desc3 = Schema()
      .deriveFieldsAlphabetically()
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())

    util.Arrays.asList(desc1, desc2, desc3)
  }

  override def validator(): DescriptorValidator = {
    new SchemaValidator(isStreamEnvironment = true)
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props1 = Map(
      "schema.property-version" -> "1",
      "schema.fields.0.name" -> "myField",
      "schema.fields.0.type" -> "BOOLEAN",
      "schema.fields.1.name" -> "otherField",
      "schema.fields.1.type" -> "VARCHAR",
      "schema.fields.1.from" -> "csvField",
      "schema.fields.2.name" -> "p",
      "schema.fields.2.type" -> "TIMESTAMP",
      "schema.fields.2.proctime" -> "true",
      "schema.fields.3.name" -> "r",
      "schema.fields.3.type" -> "TIMESTAMP",
      "schema.fields.3.rowtime.watermarks.type" -> "from-source",
      "schema.fields.3.rowtime.timestamps.type" -> "from-source"
    )

    val props2 = Map(
      "schema.property-version" -> "1",
      "schema.fields.0.name" -> "myField",
      "schema.fields.0.type" -> "BOOLEAN",
      "schema.fields.1.name" -> "otherField",
      "schema.fields.1.type" -> "VARCHAR",
      "schema.fields.1.from" -> "csvField",
      "schema.fields.2.name" -> "p",
      "schema.fields.2.type" -> "TIMESTAMP",
      "schema.fields.2.proctime" -> "true",
      "schema.fields.3.name" -> "r",
      "schema.fields.3.type" -> "TIMESTAMP"
    )

    val props3 = Map(
      "schema.property-version" -> "1",
      "schema.derive-fields" -> "alphabetically",
      "schema.fields.0.name" -> "p",
      "schema.fields.0.type" -> "TIMESTAMP",
      "schema.fields.0.proctime" -> "true",
      "schema.fields.1.name" -> "r",
      "schema.fields.1.type" -> "TIMESTAMP",
      "schema.fields.1.rowtime.watermarks.type" -> "from-source",
      "schema.fields.1.rowtime.timestamps.type" -> "from-source"
    )

    util.Arrays.asList(props1.asJava, props2.asJava, props3.asJava)
  }
}
