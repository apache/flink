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

import akka.actor.{Props, ActorSystem}
import akka.testkit.TestKit
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.{JobStatus, JobID, JobGraph, AbstractJobVertex}
import org.apache.flink.runtime.jobmanager.Tasks
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ExecutionGraphRestartTest(_system: ActorSystem) extends TestKit(_system) with WordSpecLike
with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val NUM_TASKS = 31

  "The execution graph" must {
    "be manually restartable" in {
      try {
        val tm = system.actorOf(Props(classOf[ExecutionGraphTestUtils
        .SimpleAcknowledgingTaskManager], "TaskManager"))
        val instance = ExecutionGraphTestUtils.getInstance(tm)

        val scheduler = new Scheduler
        scheduler.newInstanceAvailable(instance)

        val sender = new AbstractJobVertex("Task")
        sender.setInvokableClass(classOf[Tasks.NoOpInvokable])
        sender.setParallelism(NUM_TASKS)

        val jobGraph = new JobGraph("Pointwise job", sender)

        val eg = new ExecutionGraph(new JobID(), "test job", new Configuration(),
          AkkaUtils.getDefaultTimeout)
        eg.setNumberOfRetriesLeft(0)
        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources)

        eg.getState should equal(JobStatus.CREATED)

        eg.scheduleForExecution(scheduler)
        eg.getState should equal(JobStatus.RUNNING)

        eg.getAllExecutionVertices.iterator().next().fail(new Exception("Test Exception"))
        eg.getState should equal(JobStatus.FAILED)

        eg.restart()
        eg.getState should equal(JobStatus.RUNNING)

        import collection.JavaConverters._
        for (vertex <- eg.getAllExecutionVertices.asScala) {
          vertex.executionFinished()
        }

        eg.getState should equal(JobStatus.FINISHED)
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      }
    }

    "restart itself automatically" in {
      try {
        val tm = system.actorOf(Props
          (classOf[ExecutionGraphTestUtils.SimpleAcknowledgingTaskManager], "TaskManager"))
        val instance = ExecutionGraphTestUtils.getInstance(tm)

        val scheduler = new Scheduler
        scheduler.newInstanceAvailable(instance)

        val sender = new AbstractJobVertex("Task")
        sender.setInvokableClass(classOf[Tasks.NoOpInvokable])
        sender.setParallelism(NUM_TASKS)

        val jobGraph = new JobGraph("Pointwise job", sender)

        val eg = new ExecutionGraph(new JobID(), "Test job", new Configuration(),
          AkkaUtils.getDefaultTimeout)
        eg.setNumberOfRetriesLeft(1)
        eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources)

        eg.getState should equal(JobStatus.CREATED)

        eg.scheduleForExecution(scheduler)
        eg.getState should equal(JobStatus.RUNNING)

        eg.getAllExecutionVertices.iterator().next().fail(new Exception("Test Exception"))

        eg.getState should equal(JobStatus.RUNNING)

        import collection.JavaConverters._
        for (vertex <- eg.getAllExecutionVertices.asScala) {
          vertex.executionFinished()
        }

        eg.getState should equal(JobStatus.FINISHED)
      }catch{
        case t: Throwable =>
          t.printStackTrace()
          fail(t.getMessage)
      }
    }
  }

}
