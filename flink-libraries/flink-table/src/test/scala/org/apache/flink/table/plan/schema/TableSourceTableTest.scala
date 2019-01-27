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

package org.apache.flink.table.plan.schema

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.{TableSchemaUtil, TestTableSourceTable}

import org.junit.Test
import org.junit.Assert._

class TableSourceTableTest {

  @Test
  def testUniqueKeys {
    val source = new TestTableSourceTable(new TestTableSource)
    assertNull(source.getStatistic.asInstanceOf[FlinkStatistic].getUniqueKeys)

    val uniqueKeys = ImmutableSet.of(ImmutableSet.copyOf(Array[String]("a")))
    val sourceWithUniqueKeys =
      new TestTableSourceTable(
        new TestTableSource,
        FlinkStatistic.builder.uniqueKeys(uniqueKeys).build())
    assertEquals(
      sourceWithUniqueKeys.getStatistic.asInstanceOf[FlinkStatistic].getUniqueKeys, uniqueKeys)

    val sourceWithPrimaryKeys =
      new TestTableSourceTable(new TestTableSourceWithSchema)
    assertEquals(
      sourceWithPrimaryKeys.getStatistic.asInstanceOf[FlinkStatistic].getUniqueKeys, uniqueKeys)

  }

  class TestTableSource extends TableSource {
    /** Returns the [[TypeInformation]] for the return type of the [[TableSource]]. */
    override def getReturnType: DataType =
      new RowTypeInfo(Array[TypeInformation[_]](Types.LONG, Types.STRING), Array("a", "b"))

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(getReturnType)
  }

  class TestTableSourceWithSchema extends TableSource {
    /** Returns the [[TypeInformation]] for the return type of the [[TableSource]]. */
    override def getReturnType: DataType =
      new RowTypeInfo(Array[TypeInformation[_]](Types.LONG, Types.STRING), Array("a", "b"))

    override def getTableSchema: TableSchema = {
      TableSchema.builder()
          .field("a", DataTypes.LONG, false)
          .field("b", DataTypes.STRING, true)
          .primaryKey("a")
          .build()
    }
  }
}
