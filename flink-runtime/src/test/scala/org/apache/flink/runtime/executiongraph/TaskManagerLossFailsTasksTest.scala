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

package org.apache.flink.runtime.executiongraph

import java.util.concurrent.Executors

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy
import org.apache.flink.runtime.jobgraph.{JobGraph, JobStatus, JobVertex}
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.apache.flink.runtime.testtasks.NoOpInvokable
import org.apache.flink.util.SerializedValue

import org.junit.runner.RunWith

import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class TaskManagerLossFailsTasksTest extends WordSpecLike with Matchers {

  "A task manager loss" must {
    "fail the assigned tasks" in {
      val executor = Executors.newScheduledThreadPool(1)

      try {
        val instance1 = ExecutionGraphTestUtils.getInstance(
          new ActorTaskManagerGateway(new SimpleActorGateway(TestingUtils.defaultExecutionContext)),
          10)
        val instance2 = ExecutionGraphTestUtils.getInstance(
          new ActorTaskManagerGateway(new SimpleActorGateway(TestingUtils.defaultExecutionContext)),
          10)

        val scheduler = new Scheduler(TestingUtils.defaultExecutionContext)
        scheduler.newInstanceAvailable(instance1)
        scheduler.newInstanceAvailable(instance2)

        val sender = new JobVertex("Task")
        sender.setInvokableClass(classOf[NoOpInvokable])
        sender.setParallelism(20)

        val jobGraph = new JobGraph("Pointwise job", sender)

        val eg = new ExecutionGraph(
          executor,
          executor,
          new JobID(),
          "test job",
          new Configuration(),
          new SerializedValue(new ExecutionConfig()),
          AkkaUtils.getDefaultTimeout,
          new NoRestartStrategy(),
          scheduler)

        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources)

        eg.getState should equal(JobStatus.CREATED)

        eg.scheduleForExecution()
        eg.getState should equal(JobStatus.RUNNING)

        instance1.markDead()
        eg.getState should equal(JobStatus.FAILING)
      } catch {
        case t:Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      }
      finally {
        executor.shutdownNow()
      }
    }
  }
}
