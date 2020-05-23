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

package org.apache.flink.table.planner.catalog

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment, TableSchema}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.junit.Test
import org.junit.Assert.assertEquals

/**
  * Unit tests around catalog table and DDL.
  */
class CatalogTableTest {

  val tEnv: TableEnvironment = TableEnvironmentImpl.create(
    EnvironmentSettings.newInstance().inStreamingMode().build())

  @Test
  def testDDLSchema(): Unit = {
    tEnv.executeSql(
      """
        |CREATE TABLE t1 (
        |  f1 INT,
        |  f2 BIGINT NOT NULL,
        |  f3 STRING,
        |  f4 DECIMAL(10, 4),
        |  f5 TIMESTAMP(2) NOT NULL,
        |  f6 TIME,
        |  f7 DATE,
        |  f8 VARCHAR(10) NOT NULL
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    )

    val actual = tEnv.sqlQuery("SELECT * FROM t1").getSchema
    val expected = TableSchema.builder()
      .field("f1", DataTypes.INT())
      .field("f2", DataTypes.BIGINT().notNull())
      .field("f3", DataTypes.STRING())
      .field("f4", DataTypes.DECIMAL(10, 4))
      .field("f5", DataTypes.TIMESTAMP(2).notNull())
      .field("f6", DataTypes.TIME())
      .field("f7", DataTypes.DATE())
      .field("f8", DataTypes.VARCHAR(10).notNull())
      .build()

    assertEquals(expected, actual)
  }

}
