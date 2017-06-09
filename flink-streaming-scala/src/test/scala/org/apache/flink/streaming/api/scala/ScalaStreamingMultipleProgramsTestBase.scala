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

import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.test.util.TestBaseUtils

import org.junit.{After, Before}

import org.scalatest.junit.JUnitSuiteLike

trait ScalaStreamingMultipleProgramsTestBase
  extends TestBaseUtils
  with  JUnitSuiteLike {

  val parallelism = 4
  var cluster: Option[LocalFlinkMiniCluster] = None

  @Before
  def beforeAll(): Unit = {
    val cluster = Some(
      TestBaseUtils.startCluster(
        1,
        parallelism,
        false,
        false,
        true
      )
    )

    TestStreamEnvironment.setAsContext(cluster.get, parallelism)
  }

  @After
  def afterAll(): Unit = {
    TestStreamEnvironment.unsetAsContext()
    cluster.foreach {
      TestBaseUtils.stopCluster(_, TestBaseUtils.DEFAULT_TIMEOUT)
    }
  }
}
