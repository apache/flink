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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendRowDataSink}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{IntType, VarCharType}

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ValuesITCase extends StreamingTestBase {

  @Test
  def testValues(): Unit = {

    val sqlQuery = "SELECT * FROM (VALUES (1, 'Bob'), (1, 'Alice')) T(a, b)"

    val outputType = InternalTypeInfo.ofFields(new IntType(), new VarCharType(5))

    val table = tEnv.sqlQuery(sqlQuery)
    val sink = new TestingAppendRowDataSink(outputType)
    tEnv.toDataStream(table, outputType.getDataType).addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("+I(1,Alice)", "+I(1,Bob)")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }
}
