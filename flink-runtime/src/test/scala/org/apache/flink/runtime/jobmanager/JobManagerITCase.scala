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

import Tasks._
import akka.actor.ActorSystem
import akka.actor.Status.{Success, Failure}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.jobgraph.{JobVertex, DistributionPattern, JobGraph, ScheduleMode}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.testingUtils.{ScalaTestingUtils, TestingUtils}
import org.apache.flink.runtime.util.SerializedThrowable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.apache.flink.runtime.jobmanager.scheduler.{NoResourceAvailableException, SlotSharingGroup}

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

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = TestingUtils.startTestingCluster(1)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        val response = jmGateway.ask(RequestTotalNumberOfSlots, timeout.duration).mapTo[Int]

        val availableSlots = Await.result(response, duration)

        availableSlots should equal(1)

        within(2 second) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        val response = jmGateway.ask(RequestTotalNumberOfSlots, timeout.duration).mapTo[Int]

        val availableSlots = Await.result(response, duration)

        availableSlots should equal(num_tasks)

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Test job", vertex)
      jobGraph.setAllowQueuedScheduling(true)

      val cluster = TestingUtils.startTestingCluster(10)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Bipartite Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = TestingUtils.startTestingCluster(6 * num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Forwarding Job", sender, forwarder, receiver)

      jobGraph.setScheduleMode(ScheduleMode.ALL)

      val cluster = TestingUtils.startTestingCluster(num_tasks, 1)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Pointwise Job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)
        }

        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(2 * num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)

          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("Pointwise job", sender, receiver)

      val cluster = TestingUtils.startTestingCluster(num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try {
        within(TestingUtils.TESTING_DURATION) {
          jmGateway.tell(RequestTotalNumberOfSlots, self)
          expectMsg(num_tasks)

          jmGateway.tell(SubmitJob(jobGraph, false), self)
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

      val jobGraph = new JobGraph("SubtaskInFinalStateRaceCondition", source, sink)

      val cluster = TestingUtils.startTestingCluster(2*num_tasks)
      val jmGateway = cluster.getJobManagerGateway()

      try{
        within(TestingUtils.TESTING_DURATION){
          jmGateway.tell(SubmitJob(jobGraph, false), self)

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
  }

  class WaitingOnFinalizeJobVertex(name: String, val waitingTime: Long) extends JobVertex(name){
    var finished = false

    override def finalizeOnMaster(loader: ClassLoader): Unit = {
      Thread.sleep(waitingTime)
      finished = true
    }
  }
}
