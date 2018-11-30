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

package org.apache.flink.runtime.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.apache.flink.configuration._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingCluster, TestingUtils}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.junit.JUnitRunner

/**
  * Testing the flink cluster using SSL transport for akka remoting
  */
@RunWith(classOf[JUnitRunner])
class AkkaSslITCase(_system: ActorSystem)
  extends TestKit(_system)
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaTestingUtils {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The flink Cluster" must {

    "start with akka ssl enabled" in {

      val config = new Configuration()
      config.setString(JobManagerOptions.ADDRESS, "127.0.0.1")
      config.setString(TaskManagerOptions.HOST, "127.0.0.1")
      config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1)
      config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)

      config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)
      config.setString(SecurityOptions.SSL_KEYSTORE,
        getClass.getResource("/local127.keystore").getPath)
      config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password")
      config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password")
      config.setString(SecurityOptions.SSL_TRUSTSTORE,
        getClass.getResource("/local127.truststore").getPath)

      config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password")

      val cluster = new TestingCluster(config, false)

      cluster.start(true)

      assert(cluster.running)
    }

    "Failed to start ssl enabled akka with two protocols set" in {

      an[Exception] should be thrownBy {

        val config = new Configuration()
        config.setString(JobManagerOptions.ADDRESS, "127.0.0.1")
        config.setString(TaskManagerOptions.HOST, "127.0.0.1")
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1)
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)

        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)
        config.setString(SecurityOptions.SSL_KEYSTORE,
          getClass.getResource("/local127.keystore").getPath)
        config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password")
        config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password")
        config.setString(SecurityOptions.SSL_TRUSTSTORE,
          getClass.getResource("/local127.truststore").getPath)

        config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password")
        config.setString(SecurityOptions.SSL_ALGORITHMS, "TLSv1,TLSv1.1")

        val cluster = new TestingCluster(config, false)

        cluster.start(true)
      }
    }

    "start with akka ssl disabled" in {

      val config = new Configuration()
      config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1)
      config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
      config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false)

      val cluster = new TestingCluster(config, false)

      cluster.start(true)

      assert(cluster.running)
    }

    "fail to start with invalid ssl keystore configured" in {

      an[Exception] should be thrownBy {

        val config = new Configuration()
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1)
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
        config.setString(AkkaOptions.ASK_TIMEOUT, "2 s")

        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)
        config.setString(SecurityOptions.SSL_KEYSTORE, "invalid.keystore")
        config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password")
        config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password")
        config.setString(SecurityOptions.SSL_TRUSTSTORE, "invalid.keystore")
        config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password")

        val cluster = new TestingCluster(config, false)

        cluster.start(true)
      }
    }

    "fail to start with missing mandatory ssl configuration" in {

      an[Exception] should be thrownBy {

        val config = new Configuration()
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1)
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
        config.setString(AkkaOptions.ASK_TIMEOUT, "2 s")

        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true)

        val cluster = new TestingCluster(config, false)

        cluster.start(true)
      }
    }

  }

}
