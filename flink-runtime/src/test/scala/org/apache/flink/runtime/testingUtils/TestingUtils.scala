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

package org.apache.flink.runtime.testingUtils

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.CallingThreadDispatcher

import com.typesafe.config.ConfigFactory

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.ActionQueue

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

/**
 * Convenience functions to test actor based components.
 */
object TestingUtils {

  val testConfig = ConfigFactory.parseString(getDefaultTestingActorSystemConfigString)

  val TESTING_DURATION = 2 minute

  val DEFAULT_AKKA_ASK_TIMEOUT = "200 s"

  def getDefaultTestingActorSystemConfigString: String = {
    val logLevel = AkkaUtils.getLogLevel

    s"""akka.daemonic = on
      |akka.test.timefactor = 10
      |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
      |akka.loglevel = $logLevel
      |akka.stdout-loglevel = OFF
      |akka.jvm-exit-on-fatal-error = off
      |akka.log-config-on-start = off
    """.stripMargin
  }

  def getDefaultTestingActorSystemConfig = testConfig
  

  def startTestingCluster(numSlots: Int, numTMs: Int = 1,
                          timeout: String = DEFAULT_AKKA_ASK_TIMEOUT): TestingCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTMs)
    config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, timeout)
    new TestingCluster(config)
  }

  def setGlobalExecutionContext(): Unit = {
    AkkaUtils.globalExecutionContext = ExecutionContext.global
  }

  def setCallingThreadDispatcher(system: ActorSystem): Unit = {
    AkkaUtils.globalExecutionContext = system.dispatchers.lookup(CallingThreadDispatcher.Id)
  }

  def setExecutionContext(context: ExecutionContext): Unit = {
    AkkaUtils.globalExecutionContext = context
  }

  class QueuedActionExecutionContext(queue: ActionQueue) extends ExecutionContext {
    var automaticExecution = false

    def toggleAutomaticExecution() = {
      automaticExecution = !automaticExecution
    }

    override def execute(runnable: Runnable): Unit = {
      if(automaticExecution){
        runnable.run()
      }else {
        queue.queueAction(runnable)
      }
    }

    override def reportFailure(t: Throwable): Unit = {
      t.printStackTrace()
    }
  }
}
