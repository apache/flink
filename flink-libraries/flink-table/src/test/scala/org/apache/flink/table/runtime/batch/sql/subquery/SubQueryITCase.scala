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

package org.apache.flink.table.runtime.batch.sql.subquery

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.conversion.DataStructureConverters
import org.junit._

import scala.collection.Seq

class SubQueryITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    DataStructureConverters.createToExternalConverter(DataTypes.createRowType())
    registerCollection("t1", Seq(row(1, 2, 3)),
      new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO), "t1a, t1b, t1c")
    registerCollection("t2", Seq(row(1, 0, 1)),
      new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO), "t2a, t2b, t2c")
    registerCollection("t3", Seq(row(3, 1, 2)),
      new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO), "t3a, t3b, t3c")
  }

  @Test
  def testCorrelatedFieldInAggCall(): Unit = {
    checkResult(
      """
        |SELECT t1a
        |FROM   t1
        |WHERE  t1a IN (SELECT t2a
        |               FROM   t2
        |               WHERE  EXISTS (SELECT min(t2a)
        |                              FROM   t3))
      """.stripMargin, Seq(row(1)))
  }
}
