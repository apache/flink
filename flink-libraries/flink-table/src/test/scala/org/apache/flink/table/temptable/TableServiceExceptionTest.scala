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

package org.apache.flink.table.temptable

import java.io.File
import java.nio.file.Files

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.junit.{Assert, Ignore, Test}

@Ignore
class TableServiceExceptionTest extends BatchTestBase {

  @Test
  def testTableServiceUnavailable(): Unit = {
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    tEnv.getConfig.setSubsectionOptimization(true)
    val path = new File(System.getProperty("user.dir")).toPath
    val rootPath = Files.createTempDirectory(
      path,
      "testTableServiceUnavailable_" + System.nanoTime())
      .toAbsolutePath.toString
    val descriptor = tEnv.getConfig.getTableServiceDescriptor()
    descriptor.getConfiguration.setString(
      TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH,
      rootPath
    )
    tEnv.getConfig.setTableServiceDescriptor(descriptor)

    val data = List[(Int, Int)] (
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 5)
    )

    val source = tEnv.fromCollection(data).as('a, 'b)
    val filteredTable = source.filter('a < 5)

    filteredTable.cache()

    filteredTable.collect().size

    val cachedName = tEnv.tableServiceManager.getCachedTableName(filteredTable.logicalPlan).get


    val baseDir = new File(rootPath)

    val currentTableServiceDir = searchDir(baseDir, cachedName)

    // delete exist cache
    deleteAll(currentTableServiceDir)
    Assert.assertTrue(!currentTableServiceDir.exists())

    val result = filteredTable.select('a + 1 as 'a)

    // this action will fail due to missing cache and will fallback to original plan
    val res = result.collect()

    Assert.assertTrue(res.size == 4)

    // cache has been re-computed by original plan.
    Assert.assertTrue(currentTableServiceDir.exists())
    Assert.assertEquals(List(2, 3, 4, 5).mkString("\n"), res.map(_.toString).mkString("\n"))

    tEnv.close()
  }

  private def deleteAll(dir: File): Unit = {
    if (dir.isFile) {
      dir.delete()
    } else {
      dir.listFiles().foreach(deleteAll(_))
      dir.delete()
    }
  }

  private def searchDir(base: File, tableName: String): File = {
    base.listFiles.find(
      subDir => if (subDir.isDirectory) {
        subDir.listFiles().exists(f => f.isDirectory && f.getName == tableName)
      } else {
        false
      }
    ).get
  }

}
