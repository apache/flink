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
package org.apache.flink.table.api.validation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class TableSchemaValidationTest extends TableTestBase {

  @Test
  def testColumnNameAndColumnTypeNotEqual() {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Number of field names and field types must be equal.\n" +
        "Number of names is 3, number of types is 2.\n" +
        "List of field names: [a, b, c].\n" +
        "List of field types: [Integer, String].")

    val fieldNames = Array("a", "b", "c")
    val typeInfos: Array[TypeInformation[_]] = Array(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    new TableSchema(fieldNames, typeInfos)
  }

  @Test
  def testColumnNamesDuplicate() {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Field names must be unique.\n" +
        "List of duplicate fields: [a].\n" +
        "List of all fields: [a, a, c].")

    val fieldNames = Array("a", "a", "c")
    val typeInfos: Array[TypeInformation[_]] = Array(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    new TableSchema(fieldNames, typeInfos)
  }
}
