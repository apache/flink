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

package org.apache.flink.test.util

import org.scalatest.{Suite, BeforeAndAfter}

/** Mixin to start and stop a ForkableFlinkMiniCluster automatically for Scala based tests.
  * Additionally a TestEnvironment with the started cluster is created and set as the default
  * [[org.apache.flink.api.java.ExecutionEnvironment]].
  *
  * This mixin starts a ForkableFlinkMiniCluster with one TaskManager and a number of slots given
  * by parallelism. This value can be overridden in a sub class in order to start the cluster
  * with a different number of slots.
  *
  * The cluster is started once before starting the tests and is re-used for the individual tests.
  * After all tests have been executed, the cluster is shutdown.
  *
  * The cluster is used by obtaining the default [[org.apache.flink.api.java.ExecutionEnvironment]].
  *
  * @example
  *          {{{
  *            def testSomething: Unit = {
  *             // Obtain TestEnvironment with started ForkableFlinkMiniCluster
  *             val env = ExecutionEnvironment.getExecutionEnvironment
  *
  *             env.fromCollection(...)
  *
  *             env.execute
  *            }
  *          }}}
  *
  */
trait FlinkTestBase
  extends BeforeAndAfter {
  that: Suite =>

  var cluster: Option[ForkableFlinkMiniCluster] = None
  val parallelism = 4

  before {
    val cl = TestBaseUtils.startCluster(1, parallelism, false)
    val clusterEnvironment = new TestEnvironment(cl, parallelism)
    clusterEnvironment.setAsContext

    cluster = Some(cl)
  }

  after {
    cluster.map(c => TestBaseUtils.stopCluster(c, TestBaseUtils.DEFAULT_TIMEOUT))
  }

}
