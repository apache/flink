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
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager
import org.apache.flink.runtime.io.network.api.{RecordReader, RecordWriter}
import org.apache.flink.runtime.jobgraph.{DistributionPattern, JobStatus, JobGraph, AbstractJobVertex}
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable
import org.apache.flink.runtime.jobmanager.TestingJobManagerMessages.{ExecutionGraphNotFound, ExecutionGraphFound, ResponseExecutionGraph, RequestExecutionGraph}
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusFound
import org.apache.flink.runtime.messages.JobManagerMessages.{RequestJobStatusWhenTerminated, SubmitJob, RequestAvailableSlots}
import org.apache.flink.runtime.messages.JobResult
import org.apache.flink.runtime.messages.JobResult.JobSubmissionResult
import org.apache.flink.runtime.types.IntegerRecord
import org.scalatest.{Matchers, WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

class JobManagerITCase2(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with
Matchers with BeforeAndAfterAll{
  def this() = this(ActorSystem("TestingActorSystem"))

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "The JobManager actor" must {
    "handle jobs when not enough slots" in {
      val vertex = new AbstractJobVertex("Test Vertex")
      vertex.setParallelism(2)
      vertex.setInvokableClass(classOf[BlockingNoOpInvokable])

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = startTestingCluster(1)
      val jm = cluster.getJobManager

      try{
        val availableSlots = AkkaUtils.ask[Int](jm, RequestAvailableSlots)
        availableSlots should equal(1)

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())
        val result = AkkaUtils.ask[JobSubmissionResult](jm, SubmitJob(jobGraph))

        result.returnCode should equal(JobResult.ERROR)

        within(1 second){
          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, executionGraph) => executionGraph
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
        cluster.stop()
      }
    }

    "support immediate scheduling of a single vertex" in {
      val num_tasks = 133
      val vertex = new AbstractJobVertex("Test Vertex")
      vertex.setParallelism(num_tasks)
      vertex.setInvokableClass(classOf[NoOpInvokable])

      val jobGraph = new JobGraph("Test Job", vertex)

      val cluster = startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try{
        val availableSlots = AkkaUtils.ask[Int](jm, RequestAvailableSlots)
        availableSlots should equal(num_tasks)

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        val result = AkkaUtils.ask[JobSubmissionResult](jm, SubmitJob(jobGraph))

        result.returnCode should equal(JobResult.SUCCESS)

        within(1 second){
          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(10)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)

          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated

          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(2*num_tasks)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)

          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated

          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(2* num_tasks)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second){
          jm ! SubmitJob(jobGraph)

          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated

          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
        cluster.stop()
      }
    }

    "support two input job failing edge mismatch" in {
      val num_tasks = 11
      val sender1 = new AbstractJobVertex("Sender1")
      val sender2 = new AbstractJobVertex("Sender2")
      val receiver = new AbstractJobVertex("Receiver")

      sender1.setInvokableClass(classOf[Sender])
      sender2.setInvokableClass(classOf[Sender])
      receiver.setInvokableClass(classOf[AgnosticReceiver])

      sender1.setParallelism(num_tasks)
      sender2.setParallelism(2*num_tasks)
      receiver.setParallelism(3* num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE)

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = startTestingCluster(6*num_tasks)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)

          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated

          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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
      sender2.setParallelism(2*num_tasks)
      receiver.setParallelism(3* num_tasks)

      receiver.connectNewDataSetAsInput(sender1, DistributionPattern.POINTWISE)
      receiver.connectNewDataSetAsInput(sender2, DistributionPattern.BIPARTITE)

      val jobGraph = new JobGraph("Bipartite Job", sender1, receiver, sender2)

      val cluster = startTestingCluster(6*num_tasks)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)

          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated

          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FINISHED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try{
        within(1 second) {
          jm ! RequestAvailableSlots
          expectMsg(num_tasks)
        }

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)
          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try{
        within(1 second) {
          jm ! RequestAvailableSlots
          expectMsg(num_tasks)
        }

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)
          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(2*num_tasks)
      val jm = cluster.getJobManager

      try{
        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)
          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try{
        within(1 second) {
          jm ! RequestAvailableSlots
          expectMsg(num_tasks)
        }

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)
          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
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

      val cluster = startTestingCluster(num_tasks)
      val jm = cluster.getJobManager

      try{
        within(1 second) {
          jm ! RequestAvailableSlots
          expectMsg(num_tasks)
        }

        LibraryCacheManager.register(jobGraph.getJobID, Array[String]())

        within(1 second) {
          jm ! SubmitJob(jobGraph)
          expectMsg(JobSubmissionResult(JobResult.SUCCESS, null))

          jm ! RequestJobStatusWhenTerminated
          expectMsg(JobStatusFound(jobGraph.getJobID, JobStatus.FAILED))
        }

        val executionGraph = AkkaUtils.ask[ResponseExecutionGraph](jm, RequestExecutionGraph(jobGraph.getJobID)) match {
          case ExecutionGraphFound(_, eg) => eg
          case ExecutionGraphNotFound(jobID) => fail(s"The execution graph for job ID ${jobID} was not retrievable.")
        }

        executionGraph.getRegisteredExecutions.size should equal(0)
      }finally{
        cluster.stop()
      }
    }
  }


  def startTestingCluster(slots: Int) = {
    val configuration = new Configuration()
    configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slots)

    val cluster = new TestingCluster()
    cluster.start(configuration)
    cluster
  }
}

class BlockingNoOpInvokable extends AbstractInvokable {
  override def registerInputOutput(): Unit = {}

  override def invoke(): Unit = {
    val o = new Object()
    o.synchronized{
      o.wait()
    }
  }
}

class NoOpInvokable extends AbstractInvokable{
  override def registerInputOutput(): Unit = {}

  override def invoke(): Unit = {}
}

class Sender extends AbstractInvokable{
  var writer: RecordWriter[IntegerRecord] = _
  override def registerInputOutput(): Unit = {
    writer = new RecordWriter[IntegerRecord](this)
  }

  override def invoke(): Unit = {
    try{
      writer.initializeSerializers()
      writer.emit(new IntegerRecord(42))
      writer.emit(new IntegerRecord(1337))
      writer.flush()
    }finally{
      writer.clearBuffers()
    }
  }
}

class Receiver extends AbstractInvokable {
  var reader: RecordReader[IntegerRecord] = _

  override def registerInputOutput(): Unit = {
    reader = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
  }

  override def invoke(): Unit = {
    val i1 = reader.next()
    val i2 = reader.next()
    val i3 = reader.next()

    if(i1.getValue != 42 || i2.getValue != 1337 || i3 != null){
      throw new Exception("Wrong data received.")
    }
  }
}

class AgnosticReceiver extends AbstractInvokable {
  var reader: RecordReader[IntegerRecord] = _

  override def registerInputOutput(): Unit = {
    reader = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
  }

  override def invoke(): Unit = {
    while(reader.next() != null){}
  }
}

class AgnosticBinaryReceiver extends AbstractInvokable {
  var reader1: RecordReader[IntegerRecord] = _
  var reader2: RecordReader[IntegerRecord] = _

  override def registerInputOutput(): Unit = {
    reader1 = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
    reader2 = new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
  }

  override def invoke(): Unit = {
    while(reader1.next() != null){}
    while(reader2.next() != null){}
  }
}

class ExceptionSender extends AbstractInvokable{
  var writer: RecordWriter[IntegerRecord] = _

  override def registerInputOutput(): Unit = {
    writer = new RecordWriter[IntegerRecord](this)
  }

  override def invoke(): Unit = {
    writer.initializeSerializers()

    throw new Exception("Test exception")
  }
}

class SometimesExceptionSender extends AbstractInvokable {
  var writer: RecordWriter[IntegerRecord] = _

  override def registerInputOutput(): Unit = {
    writer = new RecordWriter[IntegerRecord](this)
  }

  override def invoke(): Unit = {
    writer.initializeSerializers()

    if(Math.random() < 0.05){
      throw new Exception("Test exception")
    }else{
      val o = new Object()
      o.synchronized(o.wait())
    }
  }
}

class ExceptionReceiver extends AbstractInvokable {
  override def registerInputOutput(): Unit = {
    new RecordReader[IntegerRecord](this, classOf[IntegerRecord])
  }

  override def invoke(): Unit = {
    throw new Exception("Test exception")
  }
}

class InstantiationErrorSender extends AbstractInvokable{
  throw new RuntimeException("Test exception in constructor")

  override def registerInputOutput(): Unit = {
  }

  override def invoke(): Unit = {
  }
}

class SometimesInstantiationErrorSender extends AbstractInvokable{
  if(Math.random < 0.05){
    throw new RuntimeException("Test exception in constructor")
  }

  override def registerInputOutput(): Unit = {
    new RecordWriter[IntegerRecord](this)
  }

  override def invoke(): Unit = {
    val o = new Object()
    o.synchronized(o.wait())
  }
}