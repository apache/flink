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

package org.apache.flink.streaming.api.scala

import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.test.util.{ForkableFlinkMiniCluster, TestBaseUtils}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitSuiteLike

trait ScalaStreamingMultipleProgramsTestBase
  extends TestBaseUtils
  with  JUnitSuiteLike
  with BeforeAndAfterAll {

  val parallelism = 4
  var cluster: Option[ForkableFlinkMiniCluster] = None

  override protected def beforeAll(): Unit = {
    val cluster = Some(
      TestBaseUtils.startCluster(
        1,
        parallelism,
        false,
        false,
        true
      )
    )

    val clusterEnvironment = new TestStreamEnvironment(cluster.get, parallelism)
  }

  override protected def afterAll(): Unit = {
    cluster.foreach {
      TestBaseUtils.stopCluster(_, TestBaseUtils.DEFAULT_TIMEOUT)
    }
  }
}
