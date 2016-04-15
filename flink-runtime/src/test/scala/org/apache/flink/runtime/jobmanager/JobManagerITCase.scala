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


import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.runtime.akka.ListeningBehaviour
import org.apache.flink.runtime.checkpoint.{CheckpointCoordinator, SavepointCoordinator}
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings
import org.apache.flink.runtime.jobgraph.{DistributionPattern, JobGraph, JobVertex, ScheduleMode}
import org.apache.flink.runtime.jobmanager.Tasks._
import org.apache.flink.runtime.jobmanager.scheduler.{NoResourceAvailableException, SlotSharingGroup}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils
import org.junit.runner.RunWith
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

    "handle jobs when not enough slots" in {
      val vertex = new JobVertex("Test Vertex")
      vertex.setParallelism(2)
      vertex.setInvokableClass(classOf[BlockingNoOpInvokable])

      val jobGraph = new JobGraph("Test Job", new ExecutionConfig(), vertex)

      val cluster = TestingUtils.startTestingCluster(1)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        val response = jmGateway.ask(RequestTotalNumberOfSlots, timeout.duration).mapTo[Int]

        val availableSlots = Await.result(response, duration)

        availableSlots should equal(1)

        within(2 second) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))
        }

        within(2 second) {
          val response = expectMsgType[JobResultFailure]
          val exception = response.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)
              new NoResourceAvailableException(1,1,0) should equal(e.getCause())
            case e => fail(s"Received wrong exception of type $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      }
      finally {
        cluster.stop()
      }
    }

    "support immediate scheduling of a single vertex" in {
      val num_tasks = 133
      val vertex = new JobVertex("Test Vertex")
      vertex.setParallelism(num_tasks)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test Job", new ExecutionConfig(), vertex)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        val response = jmGateway.ask(RequestTotalNumberOfSlots, timeout.duration).mapTo[Int]

        val availableSlots = Await.result(response, duration)

        availableSlots should equal(num_tasks)

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))
          
          val result = expectMsgType[JobResultSuccess]
          result.result.getJobId() should equal(jobGraph.getJobID)
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support queued scheduling of a single vertex" in {
      val num_tasks = 111

      val vertex = new JobVertex("Test Vertex")
      vertex.setParallelism(num_tasks)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test job", new ExecutionConfig(), vertex)
      jobGraph.setAllowQueuedScheduling(true)

      val cluster = TestingUtils.startTestingCluster(10)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph.getJobID)
        }
        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support forward jobs" in {
      val num_tasks = 31
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.result.getJobId() should equal(jobGraph.getJobID)
        }
        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support bipartite job" in {
      val num_tasks = 31
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Bipartite Job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          expectMsgType[JobResultSuccess]
        }
        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support two input job failing edge mismatch" in {
      val num_tasks = 1
      val sender1 = new JobVertex("Sender1")
      val sender2 = new JobVertex("Sender2")
      val receiver = new JobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticTertiaryReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(2 * num_tasks)
      receiver.setParallelism(3 * num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.ALL_TO_ALL)

      val jobGraph = new JobGraph("Bipartite Job", new ExecutionConfig(),
        sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))
          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())

          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support two input job" in {
      val num_tasks = 11
      val sender1 = new JobVertex("Sender1")
      val sender2 = new JobVertex("Sender2")
      val receiver = new JobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticBinaryReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(2 * num_tasks)
      receiver.setParallelism(3 * num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.ALL_TO_ALL)

      val jobGraph = new JobGraph("Bipartite Job", new ExecutionConfig(),
        sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          expectMsgType[JobResultSuccess]
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support scheduling all at once" in {
      val num_tasks = 16
      val sender = new JobVertex("Sender")
      val forwarder = new JobVertex("Forwarder")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      forwarder.setInvokableClass(classOf[Forwarder])
      receiver.setInvokableClass(classOf[AgnosticReceiver])

      sender.setParallelism(num_tasks)
      forwarder.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      val sharingGroup = new SlotSharingGroup(sender.getID, receiver.getID)
      sender.setSlotSharingGroup(sharingGroup)
      forwarder.setSlotSharingGroup(sharingGroup)
      receiver.setSlotSharingGroup(sharingGroup)

      forwarder.connectNewDataSetAsInput(sender, DistributionPattern.ALL_TO_ALL)
      receiver.connectNewDataSetAsInput(forwarder, DistributionPattern.ALL_TO_ALL)

      val jobGraph = new JobGraph("Forwarding Job", new ExecutionConfig(),
        sender, forwarder, receiver)

      jobGraph.setScheduleMode(ScheduleMode.ALL)

      val cluster = TestingUtils.startTestingCluster(num_tasks, 1)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          expectMsgType[JobResultSuccess]

          jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
          expectMsg(true)
        }
      } finally {
        cluster.stop()
      }
    }

    "handle job with a failing sender vertex" in {
      val num_tasks = 100
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[ExceptionSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with an occasionally failing sender vertex" in {
      val num_tasks = 100
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[SometimesExceptionSender])
      receiver.setInvokableClass(classOf[Receiver])

      // set failing senders
      SometimesExceptionSender.failingSenders = Seq.fill(10)(Random.nextInt(num_tasks)).toSet

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with a failing receiver vertex" in {
      val num_tasks = 200
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[ExceptionReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with all vertices failing during instantiation" in {
      val num_tasks = 200
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[InstantiationErrorSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)

          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)

        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with some vertices failing during instantiation" in {
      val num_tasks = 200
      val sender = new JobVertex("Sender")
      val receiver = new JobVertex("Receiver")

      sender.setInvokableClass(classOf[SometimesInstantiationErrorSender])
      receiver.setInvokableClass(classOf[Receiver])

      // set the failing sender tasks
      SometimesInstantiationErrorSender.failingSenders =
        Seq.fill(10)(Random.nextInt(num_tasks)).toSet

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", new ExecutionConfig(), sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)

          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID))

          val failure = expectMsgType[JobResultFailure]
          val exception = failure.cause.deserializeError(getClass.getClassLoader())
          exception match {
            case e: JobExecutionException =>
              jobGraph.getJobID should equal(e.getJobID)

            case e => fail(s"Received wrong exception $e.")
          }
        }

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "check that all job vertices have completed the call to finalizeOnMaster before the job " +
      "completes" in {
      val num_tasks = 31

      val source = new JobVertex("Source")
      val sink = new WaitingOnFinalizeJobVertex("Sink", 500)

      source.setInvokableClass(classOf[WaitingNoOpInvokable])
      sink.setInvokableClass(classOf[NoOpInvokable])

      source.setParallelism(num_tasks)
      sink.setParallelism(num_tasks)

      val jobGraph = new JobGraph("SubtaskInFinalStateRaceCondition",
        new ExecutionConfig(), source, sink)

      val cluster = TestingUtils.startTestingCluster(2*num_tasks)
      val jmGateway = cluster.getLeaderGateway(1 seconds)

      try{
        within(TestingUtils.TESTING_DURATION){
          jmGateway.tell(SubmitJob(jobGraph, ListeningBehaviour.EXECUTION_RESULT), self)

          expectMsg(JobSubmitSuccess(jobGraph.getJobID))
          expectMsgType[JobResultSuccess]
        }

        sink.finished should equal(true)

        jmGateway.tell(NotifyWhenJobRemoved(jobGraph.getJobID), self)
        expectMsg(true)
      } finally{
        cluster.stop()
      }
    }

    "remove execution graphs when the client ends the session explicitly" in {
      val vertex = new JobVertex("Test Vertex")
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph1 = new JobGraph("Test Job", new ExecutionConfig(), vertex)

      val slowVertex = new WaitingOnFinalizeJobVertex("Long running Vertex", 2000)
      slowVertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph2 = new JobGraph("Long running Job", new ExecutionConfig(), slowVertex)

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

      val jobGraph = new JobGraph("Test Job", new ExecutionConfig(), vertex)

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

          val jobId = new JobID()

          // Trigger savepoint for non-existing job
          jobManager.tell(TriggerSavepoint(jobId), testActor)
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
          val jobGraph = new JobGraph(new ExecutionConfig(), jobVertex)

          // Submit job w/o checkpointing configured
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Trigger savepoint for job with disabled checkpointing
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID()), testActor)
          val response = expectMsgType[TriggerSavepointFailure](deadline.timeLeft)

          // Verify the response
          response.jobId should equal(jobGraph.getJobID())
          response.cause.getClass should equal(classOf[IllegalStateException])
          response.cause.getMessage should (include("disabled") or include("configured"))
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
          val jobGraph = new JobGraph(new ExecutionConfig(), jobVertex)
          jobGraph.setSnapshotSettings(new JobSnapshottingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            60000, 60000, 60000, 1))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Mock the checkpoint coordinator
          val savepointCoordinator = mock(classOf[SavepointCoordinator])
          doThrow(new Exception("Expected Test Exception"))
            .when(savepointCoordinator).triggerSavepoint(org.mockito.Matchers.anyLong())

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("savepointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, savepointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID()), testActor)
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

    "handle trigger savepoint response after failed savepoint future" in {
      val deadline = TestingUtils.TESTING_DURATION.fromNow

      val flinkCluster = TestingUtils.startTestingCluster(1, 1)

      try {
        within(deadline.timeLeft) {
          val jobManager = flinkCluster
            .getLeaderGateway(deadline.timeLeft)

          val jobVertex = new JobVertex("Blocking vertex")
          jobVertex.setInvokableClass(classOf[BlockingNoOpInvokable])
          val jobGraph = new JobGraph(new ExecutionConfig(), jobVertex)
          jobGraph.setSnapshotSettings(new JobSnapshottingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            60000, 60000, 60000, 1))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Mock the checkpoint coordinator
          val savepointCoordinator = mock(classOf[SavepointCoordinator])
          val savepointPathPromise = scala.concurrent.promise[String]
          doReturn(savepointPathPromise.future)
            .when(savepointCoordinator).triggerSavepoint(org.mockito.Matchers.anyLong())

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("savepointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, savepointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID()), testActor)

          // Fail the promise
          savepointPathPromise.failure(new Exception("Expected Test Exception"))

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
          val jobGraph = new JobGraph(new ExecutionConfig(), jobVertex)
          jobGraph.setSnapshotSettings(new JobSnapshottingSettings(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            60000, 60000, 60000, 1))

          // Submit job...
          jobManager.tell(SubmitJob(jobGraph, ListeningBehaviour.DETACHED), testActor)
          expectMsg(JobSubmitSuccess(jobGraph.getJobID()))

          // Mock the checkpoint coordinator
          val savepointCoordinator = mock(classOf[SavepointCoordinator])
          val savepointPathPromise = scala.concurrent.promise[String]
          doReturn(savepointPathPromise.future)
            .when(savepointCoordinator).triggerSavepoint(org.mockito.Matchers.anyLong())

          // Request the execution graph and set a checkpoint coordinator mock
          jobManager.tell(RequestExecutionGraph(jobGraph.getJobID), testActor)
          val executionGraph = expectMsgType[ExecutionGraphFound](
            deadline.timeLeft).executionGraph

          // Update the savepoint coordinator field
          val field = executionGraph.getClass.getDeclaredField("savepointCoordinator")
          field.setAccessible(true)
          field.set(executionGraph, savepointCoordinator)

          // Trigger savepoint for job
          jobManager.tell(TriggerSavepoint(jobGraph.getJobID()), testActor)

          // Succeed the promise
          savepointPathPromise.success("Expected test savepoint path")

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
