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

import java.util
import java.util.Collections
import java.util.concurrent._

import akka.actor.{ActorRef, Kill}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.concurrent.{ScheduledExecutor, ScheduledExecutorServiceAdapter}

import scala.concurrent.duration.{TimeUnit, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps

/**
 * Convenience functions to test actor based components.
 */
object TestingUtils {

  private var sharedExecutorInstance: ScheduledExecutorService = _

  val testConfig = ConfigFactory.parseString(getDefaultTestingActorSystemConfigString)
  
  val TESTING_DURATION = 2 minute

  val TESTING_TIMEOUT = 1 minute

  val TIMEOUT = Time.minutes(1L)

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

  def infiniteTime: Time = {
    Time.milliseconds(Integer.MAX_VALUE);
  }

  /** 
    * Gets the shared global testing execution context 
    */
  def defaultExecutionContext: ExecutionContextExecutor = {
    ExecutionContext.fromExecutor(defaultExecutor)
  }

  /**
   * Gets the shared global testing scheduled executor
   */
  def defaultExecutor: ScheduledExecutorService = {
    synchronized {
      if (sharedExecutorInstance == null || sharedExecutorInstance.isShutdown) {
        sharedExecutorInstance = Executors.newSingleThreadScheduledExecutor();
      }

      sharedExecutorInstance
    }
  }

  def defaultScheduledExecutor: ScheduledExecutor = {
    val scheduledExecutorService = defaultExecutor

    new ScheduledExecutorServiceAdapter(scheduledExecutorService)
  }

  /** Returns an [[ExecutionContext]] which uses the current thread to execute the runnable.
    *
    * @return Direct [[ExecutionContext]] which executes runnables directly
    */
  def directExecutionContext = ExecutionContext
    .fromExecutor(org.apache.flink.runtime.concurrent.Executors.directExecutor())

  /** @return A new [[QueuedActionExecutionContext]] */
  def queuedActionExecutionContext = {
    new QueuedActionExecutionContext(new ActionQueue())
  }

  /** [[ExecutionContext]] which queues [[Runnable]] up in an [[ActionQueue]] instead of
    * execution them. If the automatic execution mode is activated, then the [[Runnable]] are
    * executed.
    */
  class QueuedActionExecutionContext private[testingUtils] (val actionQueue: ActionQueue)
    extends AbstractExecutorService with ExecutionContext with ScheduledExecutorService {

    var automaticExecution = false

    def toggleAutomaticExecution() = {
      automaticExecution = !automaticExecution
    }

    override def execute(runnable: Runnable): Unit = {
      if(automaticExecution){
        runnable.run()
      }else {
        actionQueue.queueAction(runnable)
      }
    }

    override def reportFailure(t: Throwable): Unit = {
      t.printStackTrace()
    }

    override def scheduleAtFixedRate(
        command: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit): ScheduledFuture[_] = {
      throw new UnsupportedOperationException()
    }

    override def schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture[_] = {
      throw new UnsupportedOperationException()
    }

    override def schedule[V](callable: Callable[V], delay: Long, unit: TimeUnit)
        : ScheduledFuture[V] = {
      throw new UnsupportedOperationException()
    }

    override def scheduleWithFixedDelay(
        command: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit): ScheduledFuture[_] = {
      throw new UnsupportedOperationException()
    }

    override def shutdown(): Unit = ()

    override def isTerminated: Boolean = false

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = false

    override def shutdownNow(): util.List[Runnable] = Collections.emptyList()

    override def isShutdown: Boolean = false
  }

  /** Queue which stores [[Runnable]] */
  class ActionQueue {
    private val runnables = scala.collection.mutable.Queue[Runnable]()

    def triggerNextAction() {
      val r = runnables.dequeue
      r.run()
    }

    def popNextAction(): Runnable = {
      runnables.dequeue()
    }

    def queueAction(r: Runnable) {
      runnables.enqueue(r)
    }

    def isEmpty: Boolean = {
      runnables.isEmpty
    }
  }

  /** Stops the given actor by sending it a Kill message
    *
    * @param actor
    */
  def stopActor(actor: ActorRef): Unit = {
    if (actor != null) {
      actor ! Kill
    }
  }
}
