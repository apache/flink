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
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.testingUtils.{TestingCluster, TestingUtils, ScalaTestingUtils}
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
      config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "127.0.0.1")
      config.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, "127.0.0.1")
      config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)
      config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)

      config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true)
      config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE,
        getClass.getResource("/local127.keystore").getPath)
      config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password")
      config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password")
      config.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE,
        getClass.getResource("/local127.truststore").getPath)

      config.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password")

      val cluster = new TestingCluster(config, false)

      cluster.start(true)

      assert(cluster.running)
    }

    "start with akka ssl disabled" in {

      val config = new Configuration()
      config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)
      config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
      config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, false)

      val cluster = new TestingCluster(config, false)

      cluster.start(true)

      assert(cluster.running)
    }

    "fail to start with invalid ssl keystore configured" in {

      an[Exception] should be thrownBy {

        val config = new Configuration()
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
        config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "2 s")

        config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true)
        config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "invalid.keystore")
        config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password")
        config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password")
        config.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "invalid.keystore")
        config.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password")

        val cluster = new TestingCluster(config, false)

        cluster.start(true)
      }
    }

    "fail to start with missing mandatory ssl configuration" in {

      an[Exception] should be thrownBy {

        val config = new Configuration()
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)
        config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "2 s")

        config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true)

        val cluster = new TestingCluster(config, false)

        cluster.start(true)
      }
    }

  }

}
