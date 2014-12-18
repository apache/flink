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
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.{DistributionPattern, JobGraph,
AbstractJobVertex}
import Tasks._
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.testingUtils.{TestingUtils}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class JobManagerITCase(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with
WordSpecLike with Matchers with BeforeAndAfterAll {
  implicit val timeout = 1 minute

  def this() = this(ActorSystem("TestingActorSystem", TestingUtils.testConfig))

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager actor" must {
    "handle jobs when not enough slots" in {
      val vertex = new AbstractJobVertex("Test Vertex")
      vertex.setParallelism(2)
      vertex.setInvokableClass(classOf[BlockingNoOpInvokable])

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = TestingUtils.startTestingCluster(1)
      val jm = cluster.getJobManager

      try {
        val availableSlots = AkkaUtils.ask[Int](jm, RequestTotalNumberOfSlots)
        availableSlots should equal(1)

        within(1 second) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionFailure(jobGraph.getJobID, new NoResourceAvailableException(1,1)))

          expectNoMsg()
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support immediate scheduling of a single vertex" in {
      val num_tasks = 133
      val vertex = new AbstractJobVertex("Test Vertex")
      vertex.setParallelism(num_tasks)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try {
        val availableSlots = AkkaUtils.ask[Int](jm, RequestTotalNumberOfSlots)
        availableSlots should equal(num_tasks)

        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support queued scheduling of a single vertex" in {
      val num_tasks = 111

      val vertex = new AbstractJobVertex("Test Vertex")
      vertex.setParallelism(num_tasks)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test job", vertex)
      jobGraph.setAllowQueuedScheduling(true)

      val cluster = TestingUtils.startTestingCluster(10)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
        }
        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support forward jobs" in {
      val num_tasks = 31
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          val result = expectMsgType[JobResultSuccess]

          result.jobID should equal(jobGraph.getJobID)
        }
        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support bipartite job" in {
      val num_tasks = 31
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticReceiver])

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Bipartite Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultSuccess]
        }
        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support two input job failing edge mismatch" in {
      val num_tasks = 1
      val sender1 = new AbstractJobVertex("Sender1")
      val sender2 = new AbstractJobVertex("Sender2")
      val receiver = new AbstractJobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(2 * num_tasks)
      receiver.setParallelism(3 * num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE)

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "support two input job" in {
      val num_tasks = 11
      val sender1 = new AbstractJobVertex("Sender1")
      val sender2 = new AbstractJobVertex("Sender2")
      val receiver = new AbstractJobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticBinaryReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(2 * num_tasks)
      receiver.setParallelism(3 * num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE)

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))

          expectMsgType[JobResultSuccess]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with a failing sender vertex" in {
      val num_tasks = 100
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[ExceptionSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! RequestTotalNumberOfSlots
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with an occasionally failing sender vertex" in {
      val num_tasks = 100
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[SometimesExceptionSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! RequestTotalNumberOfSlots
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with a failing receiver vertex" in {
      val num_tasks = 200
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[ExceptionReceiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with all vertices failing during instantiation" in {
      val num_tasks = 200
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[InstantiationErrorSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! RequestTotalNumberOfSlots
          expectMsg(num_tasks)

          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)

        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "handle job with some vertices failing during instantiation" in {
      val num_tasks = 200
      val sender = new AbstractJobVertex("Sender")
      val receiver = new AbstractJobVertex("Receiver")

      sender.setInvokableClass(classOf[SometimesInstantiationErrorSender])
      receiver.setInvokableClass(classOf[Receiver])

      sender.setParallelism(num_tasks)
      receiver.setParallelism(num_tasks)

      receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE)

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try {
        within(TestingUtils.TESTING_DURATION) {
          jm ! RequestTotalNumberOfSlots
          expectMsg(num_tasks)

          jm ! SubmitJob(jobGraph)
          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultFailed]
        }

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally {
        cluster.stop()
      }
    }

    "check that all job vertices have completed the call to finalizeOnMaster before the job " +
      "completes" in {
      val num_tasks = 31

      val source = new AbstractJobVertex("Source")
      val sink = new WaitingOnFinalizeJobVertex("Sink", 500)

      source.setInvokableClass(classOf[WaitingNoOpInvokable])
      sink.setInvokableClass(classOf[NoOpInvokable])

      source.setParallelism(num_tasks)
      sink.setParallelism(num_tasks)

      val jobGraph = new JobGraph("SubtaskInFinalStateRaceCondition", source, sink)

      val cluster = TestingUtils.startTestingCluster(2*num_tasks)
      val jm = cluster.getJobManager

      try{
        within(TestingUtils.TESTING_DURATION){
          jm ! SubmitJob(jobGraph)

          expectMsg(SubmissionSuccess(jobGraph.getJobID))
          expectMsgType[JobResultSuccess]
        }

        sink.finished should equal(true)

        jm ! NotifyWhenJobRemoved(jobGraph.getJobID)
        expectMsg(true)
      } finally{
        cluster.stop()
      }
    }
  }

  class WaitingOnFinalizeJobVertex(name: String, val waitingTime: Long) extends
  AbstractJobVertex(name){
    var finished = false

    override def finalizeOnMaster(loader: ClassLoader): Unit = {
      Thread.sleep(waitingTime)
      finished = true
    }
  }
}
