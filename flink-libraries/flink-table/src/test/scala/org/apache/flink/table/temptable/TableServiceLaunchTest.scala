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

import org.apache.flink.streaming.api.environment.{RemoteStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.test.util.TestUtils
import org.apache.flink.test.util.TestUtils.ThrowingSupplier
import org.junit.{Assert, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TableServiceLaunchTest(env: StreamExecutionEnvironment) {

  @Test
  def testLaunchTableService(): Unit = {
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    val descriptor = tEnv.tableServiceManager.getTableServiceDescriptor().get
    val rootPath = Files.createTempDirectory(
      "testLaunchTableService_" + System.nanoTime())
      .toAbsolutePath.toString
    val rootDir = new File(rootPath)
    rootDir.delete()
    Assert.assertTrue(!rootDir.exists())

    descriptor.getConfiguration.setString(
      TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, rootPath)
    tEnv.tableServiceManager.startTableServiceJobInternally(descriptor)
    Assert.assertTrue(tEnv.tableServiceManager.tableServiceStarted)
    Assert.assertTrue(!tEnv.tableServiceManager.submitResult.isJobExecutionResult)

    TestUtils.waitUntil(
      "TableService launch timeout",
      new ThrowingSupplier[java.lang.Boolean] {
        override def apply(): java.lang.Boolean =
          rootDir.exists() && rootDir.listFiles().size > 0
      },
      3000L)

    tEnv.close()
    Assert.assertTrue(!tEnv.tableServiceManager.tableServiceStarted)
    TestUtils.waitUntil(
      "TableService close timeout",
      new ThrowingSupplier[java.lang.Boolean] {
        override def apply(): java.lang.Boolean =
          rootDir.exists() && rootDir.listFiles().size == 0
      },
      3000L)

    rootDir.delete();
  }

}

object TableServiceLaunchTest {

  @Parameterized.Parameters(name = "StreamExecutionEnvironment={0}")
  def parameters(): java.util.Collection[Array[_]] = {
    java.util.Arrays.asList(
      Array(StreamExecutionEnvironment.getExecutionEnvironment),
      Array(new RemoteStreamEnvironment("localhost", 6123))
    )
  }

}
