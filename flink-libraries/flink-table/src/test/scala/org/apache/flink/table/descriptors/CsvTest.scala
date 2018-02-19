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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.{TableSchema, Types, ValidationException}
import org.junit.Test

import scala.collection.JavaConverters._

class CsvTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidType(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "format.fields.0.type", "WHATEVER")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidField(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "format.fields.10.name", "WHATEVER")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidQuoteCharacter(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "format.quote-character", "qq")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc1 = Csv()
      .field("field1", "STRING")
      .field("field2", Types.SQL_TIMESTAMP)
      .field("field3", TypeExtractor.createTypeInfo(classOf[Class[_]]))
      .field("field4", Types.ROW(
        Array[String]("test", "row"),
        Array[TypeInformation[_]](Types.INT, Types.STRING)))
      .lineDelimiter("^")

    val desc2 = Csv()
      .schema(new TableSchema(
        Array[String]("test", "row"),
        Array[TypeInformation[_]](Types.INT, Types.STRING)))
      .quoteCharacter('#')
      .ignoreFirstLine()

    util.Arrays.asList(desc1, desc2)
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props1 = Map(
      "format.type" -> "csv",
      "format.property-version" -> "1",
      "format.fields.0.name" -> "field1",
      "format.fields.0.type" -> "STRING",
      "format.fields.1.name" -> "field2",
      "format.fields.1.type" -> "TIMESTAMP",
      "format.fields.2.name" -> "field3",
      "format.fields.2.type" -> "ANY(java.lang.Class)",
      "format.fields.3.name" -> "field4",
      "format.fields.3.type" -> "ROW(test INT, row VARCHAR)",
      "format.line-delimiter" -> "^")

    val props2 = Map(
      "format.type" -> "csv",
      "format.property-version" -> "1",
      "format.fields.0.name" -> "test",
      "format.fields.0.type" -> "INT",
      "format.fields.1.name" -> "row",
      "format.fields.1.type" -> "VARCHAR",
      "format.quote-character" -> "#",
      "format.ignore-first-line" -> "true")

    util.Arrays.asList(props1.asJava, props2.asJava)
  }

  override def validator(): DescriptorValidator = {
    new CsvValidator()
  }
}
