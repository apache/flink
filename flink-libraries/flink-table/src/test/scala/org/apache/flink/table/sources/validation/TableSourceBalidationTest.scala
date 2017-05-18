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

package org.apache.flink.table.sources.validation

import org.apache.flink.table.api.Types
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.sources.TableSourceTestBase
import org.junit.Test

class TableSourceBalidationTest extends TableSourceTestBase {

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithNullPath(): Unit = {
    CsvTableSource.builder()
      .field("myfield", Types.STRING)
      // should fail, path is not defined
      .build()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithDuplicateFieldName(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      .field("myfield", Types.STRING)
      // should fail, field name must no be duplicate
      .field("myfield", Types.INT)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithEmptyField(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      // should fail, field can be empty
      .build()
  }
}
