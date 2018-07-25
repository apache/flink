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

package org.apache.flink.table.runtime.stream.sink.queryable

import java.time.Duration
import java.util.concurrent.{ExecutionException, TimeUnit}

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.{Deadline, Time}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment}
import org.apache.flink.table.runtime.harness.HarnessTestBase.TestStreamQueryConfig
import org.apache.flink.table.sinks.queryable.QueryableTableSink
import org.apache.flink.types.Row
import org.hamcrest.core.Is
import org.junit.Assert._
import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.{Rule, Test}


class QueryableTableSinkTest extends QueryableSinkTestBase {

  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

  val _tempFolder = new TemporaryFolder
  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  val _expectedException = ExpectedException.none()
  @Rule
  def expectedException: ExpectedException = _expectedException

  def getStateBackend: StateBackend = {
    val dbPath = tempFolder.newFolder().getAbsolutePath
    val checkpointPath = tempFolder.newFolder().toURI.toString
    val backend = new RocksDBStateBackend(checkpointPath)
    backend.setDbStoragePath(dbPath)
    backend
  }

  @Test
  def testQueryableSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //name, money
    val data = List(("jeff", -1), ("dean", -2), ("jeff", 2), ("dean", 4))
    val source = new TestKVListSource[String, Int](data)

    // select name, sum(money) as sm from t group by name
    val t = env.addSource(source).toTable(tEnv, 'name, 'money)
        .groupBy("name")
        .select("name, sum(money) as sm")

    val queryableSink = new QueryableTableSink("prefix",
      new StreamQueryConfig().withIdleStateRetentionTime(Time.minutes(1), Time.minutes(7)))

    t.writeToSink(queryableSink)

    val clusterClient = QueryableSinkTestBase.miniClusterResource.getClusterClient
    val deadline = Deadline.now.plus(Duration.ofSeconds(100))

    val autoCancellableJob = new AutoCancellableJob(deadline, clusterClient, env.getJavaEnv)
    val client = new QueryableStateClient("localhost", 9084)

    try {
      val jobId = autoCancellableJob.getJobId
      val jobGraph = autoCancellableJob.getJobGraph

      clusterClient.setDetached(true)
      clusterClient.submitJob(jobGraph, classOf[QueryableTableSinkTest].getClassLoader )

      // Wait for ten seconds for processing to complete
      Thread.sleep(10000)
      val keyType = new RowTypeInfo(
        Array[TypeInformation[_]](BasicTypeInfo.STRING_TYPE_INFO),
        Array("name"))

      val stateDesc = new ValueStateDescriptor[Integer]("sm", classOf[Integer])

      val jeffMoney = client.getKvState(jobId, "prefix-sm", Row.of("jeff"), keyType,
        stateDesc)
        .get(1, TimeUnit.SECONDS)
        .value()
      assertEquals(1, jeffMoney)

      val deanMoney = client.getKvState(jobId, "prefix-sm", Row.of("dean"), keyType,
        stateDesc)
        .get(1, TimeUnit.SECONDS)
        .value()

      assertEquals(2, deanMoney)
    } finally {
      try {
        autoCancellableJob.close()
      } catch {
        case t: Throwable => log.warn("Failed to close job.", t)
      }
    }
  }

  @Test
  def testCleanupState(): Unit = {
    // Expected to throw ExecutionException, caused by UnknownKeyOrNamespaceException
    expectedException.expect(classOf[ExecutionException])
    expectedException.expectCause(Is.isA(classOf[UnknownKeyOrNamespaceException]))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //name, money
    val data = List(("jeff", -1))
    val source = new TestKVListSource[String, Int](data)

    // select name, sum(money) as sm from t group by name
    val t = env.addSource(source).toTable(tEnv, 'name, 'money)
      .groupBy("name")
      .select("name, sum(money) as sm")

    val queryableSink = new QueryableTableSink("prefix",
      new TestStreamQueryConfig(Time.milliseconds(2), Time.seconds(1)))

    t.writeToSink(queryableSink)

    val clusterClient = QueryableSinkTestBase.miniClusterResource.getClusterClient
    val deadline = Deadline.now.plus(Duration.ofSeconds(100))

    val autoCancellableJob = new AutoCancellableJob(deadline, clusterClient, env.getJavaEnv)
    val client = new QueryableStateClient("localhost", 9084)

    try {
      val jobId = autoCancellableJob.getJobId
      val jobGraph = autoCancellableJob.getJobGraph

      clusterClient.setDetached(true)
      clusterClient.submitJob(jobGraph, classOf[QueryableTableSinkTest].getClassLoader )

      // Wait for ten seconds for processing to complete
      Thread.sleep(10000)

      // Now the state should already been cleaned
      val keyType = new RowTypeInfo(
        Array[TypeInformation[_]](BasicTypeInfo.STRING_TYPE_INFO),
        Array("name"))

      val stateDesc = new ValueStateDescriptor[Integer]("sm", classOf[Integer])

      client.getKvState(jobId, "prefix-sm", Row.of("jeff"), keyType,
        stateDesc)
        .get(1, TimeUnit.SECONDS)
        .value()
    } finally {
      try {
        autoCancellableJob.close()
      } catch {
        case t: Throwable => log.warn("Failed to close job.", t)
      }
    }
  }
}

