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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.core.fs.Path
import org.apache.flink.table.catalog.{CatalogPartitionSpec, ObjectIdentifier}
import org.apache.flink.table.factories.TestManagedTableFactory
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{After, Before, Test}

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference

class CompactManagedTableTest extends TableTestBase {

  private val tableIdentifier =
    ObjectIdentifier.of("default_catalog", "default_database", "ManagedTable")
  private val testUtil = batchTestUtil()

  @Before
  def before(): Unit = {
    val tableRef = new AtomicReference[util.Map[String, String]]
    TestManagedTableFactory.MANAGED_TABLES.put(
      tableIdentifier, tableRef)
    val ddl =
      """
        |CREATE TABLE ManagedTable (
        |  a BIGINT,
        |  b INT,
        |  c VARCHAR
        |) PARTITIONED BY (b, c)
      """.stripMargin
    testUtil.tableEnv.executeSql(ddl)

    val partitionKVs = new util.LinkedHashMap[String, String]
    partitionKVs.put("b", "0")
    partitionKVs.put("c", "flink")
    val managedTableFileEntries = new util.HashMap[CatalogPartitionSpec, util.List[Path]]()
    managedTableFileEntries.put(
      new CatalogPartitionSpec(partitionKVs),
      Collections.singletonList(new Path("/foo/bar/file")))
    val fileRef = new AtomicReference[util.Map[CatalogPartitionSpec, util.List[Path]]]
    fileRef.set(managedTableFileEntries)
    TestManagedTableFactory.MANAGED_TABLE_FILE_ENTRIES.put(tableIdentifier, fileRef)
  }

  @After
  def after(): Unit = {
    val ddl = "DROP TABLE ManagedTable"
    testUtil.tableEnv.executeSql(ddl)
    TestManagedTableFactory.MANAGED_TABLE_FILE_ENTRIES.remove(tableIdentifier)
  }

  @Test
  def testExplainAlterTableCompactWithResolvedPartitionSpec(): Unit = {
    val sql = "ALTER TABLE ManagedTable PARTITION (b = 0, c = 'flink') COMPACT"
    testUtil.verifyExplainSql(sql)
  }

  @Test
  def testExplainAlterTableCompactWithUnorderedPartitionSpec(): Unit = {
    val sql = "ALTER TABLE ManagedTable PARTITION (c = 'flink', b = 0) COMPACT"
    testUtil.verifyExplainSql(sql)
  }

  @Test
  def testExplainAlterTableCompactWithoutSubordinatePartitionSpec(): Unit = {
    val sql = "ALTER TABLE ManagedTable PARTITION (b = 0) COMPACT"
    testUtil.verifyExplainSql(sql)
  }

  @Test
  def testExplainAlterTableCompactWithoutSecondaryPartitionSpec(): Unit = {
    val sql = "ALTER TABLE ManagedTable PARTITION (c = 'flink') COMPACT"
    testUtil.verifyExplainSql(sql)
  }

  @Test
  def testExplainAlterTableCompactWithoutPartitionSpec(): Unit = {
    val sql = "ALTER TABLE ManagedTable COMPACT"
    testUtil.verifyExplainSql(sql)
  }
}
