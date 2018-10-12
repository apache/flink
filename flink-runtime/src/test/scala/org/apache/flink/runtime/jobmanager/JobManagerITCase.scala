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

package org.apache.flink.runtime.jobmanager

import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.akka.ListeningBehaviour
import org.apache.flink.runtime.checkpoint._
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.io.network.partition.ResultPartitionType
import org.apache.flink.runtime.jobgraph.tasks.{CheckpointCoordinatorConfiguration, JobCheckpointingSettings}
import org.apache.flink.runtime.jobgraph.{DistributionPattern, JobGraph, JobVertex, ScheduleMode}
import org.apache.flink.runtime.jobmanager.Tasks._
import org.apache.flink.runtime.jobmanager.scheduler.{NoResourceAvailableException, SlotSharingGroup}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.apache.flink.runtime.testtasks._
import org.junit.runner.RunWith
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class JobManagerITCase(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ScalaTestingUtils {
  implicit val duration = 1 minute
  implicit val timeout = Timeout.durationToTimeout(duration)

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager actor" must {

    "remove execution graphs when the client ends the session explicitly" in {
      val vertex = new JobVertex("Test Vertex")
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph1 = new JobGraph("Test Job", vertex)

      val slowVertex = new WaitingOnFinalizeJobVertex("Long running Vertex", 2000)
      slowVertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph2 = new JobGraph("Long running Job", slowVertex)

      val cluster = TestingUtils.startTestingCluster(1)
      val jm = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          /* jobgraph1 is removed after being terminated */
          jobGraph1.setSessionTimeout(9999)
          jm.tell(SubmitJob(jobGraph1, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph1.getJobID))
          expectMsgType[JobResultSuccess]

          // should not be archived yet
          jm.tell(RequestExecutionGraph(jobGraph1.getJobID), self)
          var cachedGraph = expectMsgType[ExecutionGraphFound].executionGraph
          assert(!cachedGraph.isArchived)

          jm.tell(RemoveCachedJob(jobGraph1.getJobID), self)

          jm.tell(RequestExecutionGraph(jobGraph1.getJobID), self)
          cachedGraph = expectMsgType[ExecutionGraphFound].executionGraph
          assert(cachedGraph.isArchived)

          /* jobgraph2 is removed while running */
          jobGraph2.setSessionTimeout(9999)
          jm.tell(SubmitJob(jobGraph2, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph2.getJobID))

          // job still running
          jm.tell(RemoveCachedJob(jobGraph2.getJobID), self)

          expectMsgType[JobResultSuccess]

          // should be archived!
          jm.tell(RequestExecutionGraph(jobGraph2.getJobID), self)
          cachedGraph = expectMsgType[ExecutionGraphFound].executionGraph
          assert(cachedGraph.isArchived)
        }
      } finally {
        cluster.stop()
      }
    }

    "remove execution graphs when when the client's session times out" in {
      val vertex = new JobVertex("Test Vertex")
      vertex.setParallelism(1)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = TestingUtils.startTestingCluster(1)
      val jm = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          // try multiple times in case of flaky environments
          var testSucceeded = false
          var numTries = 0
          while(!testSucceeded && numTries < 10) {
            try {
              // should be removed immediately
              jobGraph.setSessionTimeout(0)
              jm.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
              expectMsg(JobSubmitSuccess(jobGraph.getJobID))
              expectMsgType[JobResultSuccess]

              jm.tell(RequestExecutionGraph(jobGraph.getJobID), self)
              val cachedGraph2 = expectMsgType[ExecutionGraphFound].executionGraph
              assert(cachedGraph2.isArchived)

              // removed after 2 seconds
              jobGraph.setSessionTimeout(2)

              jm.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
              expectMsg(JobSubmitSuccess(jobGraph.getJobID))
              expectMsgType[JobResultSuccess]

              // should not be archived yet
              jm.tell(RequestExecutionGraph(jobGraph.getJobID), self)
              val cachedGraph = expectMsgType[ExecutionGraphFound].executionGraph
              assert(!cachedGraph.isArchived)

              // wait until graph is archived
              Thread.sleep(3000)

              jm.tell(RequestExecutionGraph(jobGraph.getJobID), self)
              val graph = expectMsgType[ExecutionGraphFound].executionGraph
              assert(graph.isArchived)

              testSucceeded = true
            } catch {
              case e: Throwable =>
                numTries += 1
            }
          }
          if(!testSucceeded) {
            fail("Test case failed after " + numTries + " probes.")
          }
        }
      } finally {
        cluster.stop()
      }
    }

    // ------------------------------------------------------------------------
    // Savepoint messages
    // ------------------------------------------------------------------------

    "handle trigger savepoint response for non-existing job" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(0, 0)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          // we have to make sure that the job manager knows also that he is the leader
          // in case of standalone leader retrieval this can happen after the getLeaderGateway call
          val leaderFuture = jobManager.ask(NotifyWhenLeader, timeout.duration)
          Await.ready(leaderFuture, timeout.duration)

          val jobId = new JobID()

          // Trigger savepoint for non-existing job
          jobManager.tell(TriggerSavepoint(jobId, Option.apply("any")), testActor)
          val response = expectMsgType[TriggerSavepointFailure](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobId)
          response.cause.getClass should equal(classOf[IllegalArgumentException])
        }
      }
      finally {
        flinkCluster.stop()
      }
    }

    "handle trigger savepoint response for job with disabled checkpointing" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(1, 1)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          val jobVertex = new JobVertex("Blocking vertex")
          jobVertex.setInvokableClass(classOf[BlockingNoOpInvokable])
          val jobGraph = new JobGraph(jobVertex)

          // Submit job w/o checkpointing configured
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Trigger savepoint for job with disabled checkpointing
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID(), Option.apply("any")), testActor)
          val response = expectMsgType[TriggerSavepointFailure](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobGraph.getJobID())
          response.cause.getClass should equal(classOf[IllegalStateException])
          response.cause.getMessage should include("not a streaming job")
        }
      }
      finally {
        flinkCluster.stop()
      }
    }

    "handle trigger savepoint response after trigger savepoint failure" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(1, 1)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          val jobVertex = new JobVertex("Blocking vertex")
          jobVertex.setInvokableClass(classOf[BlockingNoOpInvokable])
          val jobGraph = new JobGraph(jobVertex)
          jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            new CheckpointCoordinatorConfiguration(
              60000,
              60000,
              60000,
              1,
              CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
              true),
            null))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Mock the checkpoint coordinator
          val checkpointCoordinator = mock(classOf[CheckpointCoordinator])
          doThrow(new IllegalStateException("Expected Test Exception"))
            .when(checkpointCoordinator)
            .triggerSavepoint(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString())

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("checkpointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, checkpointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID(), Option.apply("any")), testActor)
          val response = expectMsgType[TriggerSavepointFailure](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobGraph.getJobID())
          response.cause.getCause.getClass should equal(classOf[IllegalStateException])
          response.cause.getCause.getMessage should equal("Expected Test Exception")
        }
      }
      finally {
        flinkCluster.stop()
      }
    }

    "handle failed savepoint triggering" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(1, 1)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          val jobVertex = new JobVertex("Blocking vertex")
          jobVertex.setInvokableClass(classOf[BlockingNoOpInvokable])
          val jobGraph = new JobGraph(jobVertex)
          jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            new CheckpointCoordinatorConfiguration(
              60000,
              60000,
              60000,
              1,
              CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
              true),
            null))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Mock the checkpoint coordinator
          val checkpointCoordinator = mock(classOf[CheckpointCoordinator])
          doThrow(new IllegalStateException("Expected Test Exception"))
            .when(checkpointCoordinator)
            .triggerSavepoint(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString())
          val savepointPathPromise = new CompletableFuture[CompletedCheckpoint]()
          doReturn(savepointPathPromise, Nil: _*)
            .when(checkpointCoordinator)
            .triggerSavepoint(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString())

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("checkpointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, checkpointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID(), Option.apply("any")), testActor)

          // Fail the promise
          savepointPathPromise.completeExceptionally(new Exception("Expected Test Exception"))

          val response = expectMsgType[TriggerSavepointFailure](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobGraph.getJobID())
          response.cause.getCause.getClass should equal(classOf[Exception])
          response.cause.getCause.getMessage should equal("Expected Test Exception")
        }
      }
      finally {
        flinkCluster.stop()
      }
    }

    "handle trigger savepoint response after succeeded savepoint future" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(1, 1)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          val jobVertex = new JobVertex("Blocking vertex")
          jobVertex.setInvokableClass(classOf[BlockingNoOpInvokable])
          val jobGraph = new JobGraph(jobVertex)
          jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            new CheckpointCoordinatorConfiguration(
              60000,
              60000,
              60000,
              1,
              CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
              true),
            null))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Mock the checkpoint coordinator
          val checkpointCoordinator = mock(classOf[CheckpointCoordinator])
          doThrow(new IllegalStateException("Expected Test Exception"))
            .when(checkpointCoordinator)
            .triggerSavepoint(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString())

          val savepointPromise = new CompletableFuture[CompletedCheckpoint]()
          doReturn(savepointPromise, Nil: _*)
            .when(checkpointCoordinator)
            .triggerSavepoint(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString())

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("checkpointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, checkpointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID(), Option.apply("any")), testActor)

          val checkpoint = Mockito.mock(classOf[CompletedCheckpoint])
          when(checkpoint.getExternalPointer).thenReturn("Expected test savepoint path")

          // Succeed the promise
          savepointPromise.complete(checkpoint)

          val response = expectMsgType[TriggerSavepointSuccess](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobGraph.getJobID())
          response.savepointPath should equal("Expected test savepoint path")
        }
      }
      finally {
        flinkCluster.stop()
      }
    }
  }

  class WaitingOnFinalizeJobVertex(name: String, val waitingTime: Long) extends JobVertex(name){
    var finished = false

    override def finalizeOnMaster(loader: ClassLoader): Unit = {
      Thread.sleep(waitingTime)
      finished = true
    }
  }
}
