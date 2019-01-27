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

import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class TableSchemaValidationTest extends TableTestBase {

  @Test
  def testColumnNameAndColumnTypeNotEqual() {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Number of column indexes and column names must be equal." +
        "\nColumn names count is [3]" +
        "\nColumn types count is [2]" +
        "\nColumn names: [a, b, c]" +
        "\nColumn types: [IntType, StringType]")

    val fieldNames = Array("a", "b", "c")
    new TableSchema(fieldNames, Array(DataTypes.INT, DataTypes.STRING))
  }

  @Test
  def testColumnNamesDuplicate() {
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Table column names must be unique." +
        "\nThe duplicate columns are: [a]" +
        "\nAll column names: [a, a, c]")

    val fieldNames = Array("a", "a", "c")
    new TableSchema(fieldNames, Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING))
  }
}
