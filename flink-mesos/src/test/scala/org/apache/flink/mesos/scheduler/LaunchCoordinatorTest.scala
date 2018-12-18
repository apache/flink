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

package org.apache.flink.mesos.scheduler

import java.util.{Collections, UUID}
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.actor.FSM.StateTimeout
import akka.testkit._
import com.netflix.fenzo.TaskRequest.{AssignedResources, NamedResourceSetRequest}
import com.netflix.fenzo._
import com.netflix.fenzo.functions.{Action1, Action2}
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.LaunchCoordinator._
import org.apache.flink.mesos.scheduler.messages._
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.mesos.Protos.{SlaveID, TaskInfo}
import org.apache.mesos.{Protos, SchedulerDriver}
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers => MM, Mockito}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._

import org.apache.flink.mesos.Utils.range
import org.apache.flink.mesos.Utils.ranges
import org.apache.flink.mesos.Utils.scalar
import org.apache.flink.mesos.util.MesosResourceAllocation

@RunWith(classOf[JUnitRunner])
class LaunchCoordinatorTest
  extends TestKitBase
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  lazy val config: Configuration = new Configuration()
  implicit lazy val system: ActorSystem = AkkaUtils.createLocalActorSystem(config)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def randomFramework = {
    Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID.toString).build
  }

  def randomTask = {
    val taskID = Protos.TaskID.newBuilder.setValue(UUID.randomUUID.toString).build

    def generateTaskRequest = {
      new TaskRequest() {
        private[mesos] val assignedResources = new AtomicReference[TaskRequest.AssignedResources]
        override def getId: String = taskID.getValue
        override def taskGroupName: String = ""
        override def getCPUs: Double = 1.0
        override def getMemory: Double = 1024.0
        override def getNetworkMbps: Double = 0.0
        override def getDisk: Double = 0.0
        override def getPorts: Int = 1
        override def getScalarRequests = Collections.singletonMap("gpus", 1.0)
        override def getCustomNamedResources: java.util.Map[String, NamedResourceSetRequest] =
          Collections.emptyMap[String, NamedResourceSetRequest]
        override def getSoftConstraints: java.util.List[_ <: VMTaskFitnessCalculator] = null
        override def getHardConstraints: java.util.List[_ <: ConstraintEvaluator] = null
        override def getAssignedResources: AssignedResources = assignedResources.get()
        override def setAssignedResources(assignedResources: AssignedResources): Unit = {
          this.assignedResources.set(assignedResources)
        }
      }
    }

    val task: LaunchableTask = new LaunchableTask() {
      override def taskRequest: TaskRequest = generateTaskRequest
      override def launch(
          slaveId: SlaveID,
          allocation: MesosResourceAllocation): Protos.TaskInfo = {
        Protos.TaskInfo.newBuilder
          .setTaskId(taskID).setName(taskID.getValue)
          .setCommand(Protos.CommandInfo.newBuilder.setValue("whoami"))
          .setSlaveId(slaveId)
          .build()
      }
      override def toString = taskRequest.getId
    }

    (taskID, task)
  }

  def randomSlave = {
    val slaveID = Protos.SlaveID.newBuilder.setValue(UUID.randomUUID.toString).build
    val hostname = s"host-${slaveID.getValue}"
    (slaveID, hostname)
  }

  def randomOffer(frameworkID: Protos.FrameworkID, slave: (Protos.SlaveID, String)) = {
    val offerID = Protos.OfferID.newBuilder().setValue(UUID.randomUUID.toString)
    Protos.Offer.newBuilder()
      .setFrameworkId(frameworkID)
      .setId(offerID)
      .setSlaveId(slave._1)
      .setHostname(slave._2)
      .addResources(scalar("cpus", "*", 0.75))
      .addResources(scalar("mem", "*", 4096.0))
      .addResources(scalar("disk", "*", 1024.0))
      .addResources(ranges("ports", "*", range(9000, 9001)))
      .build()
  }

  def lease(offer: Protos.Offer) = {
    new Offer(offer)
  }

  /**
    * Mock a successful task assignment result matching a task to an offer.
    */
  def taskAssignmentResult(lease: VirtualMachineLease, task: TaskRequest): TaskAssignmentResult = {
    val ports = lease.portRanges().get(0)
    val assignedPorts = ports.getBeg to ports.getBeg + task.getPorts
    val r = mock(classOf[TaskAssignmentResult])
    when(r.getTaskId).thenReturn(task.getId)
    when(r.getHostname).thenReturn(lease.hostname())
    when(r.getAssignedPorts).thenReturn(
      assignedPorts.toList.asJava.asInstanceOf[java.util.List[Integer]])
    when(r.getRequest).thenReturn(task)
    when(r.isSuccessful).thenReturn(true)
    when(r.getFitness).thenReturn(1.0)
    r
  }

  /**
    * Mock a VM assignment result with the given leases and tasks.
    */
  def vmAssignmentResult(hostname: String,
                         leasesUsed: Seq[VirtualMachineLease],
                         tasksAssigned: Set[TaskAssignmentResult]): VMAssignmentResult = {
    new VMAssignmentResult(hostname, leasesUsed.asJava, tasksAssigned.asJava)
  }

  /**
    * Mock a scheduling result with the given successes and failures.
    */
  def schedulingResult(successes: Seq[VMAssignmentResult],
                       failures: Seq[TaskAssignmentResult] = Nil,
                       exceptions: Seq[Exception] = Nil,
                       leasesAdded: Int = 0,
                       leasesRejected: Int = 0): SchedulingResult = {
    val r = mock(classOf[SchedulingResult])
    when(r.getResultMap).thenReturn(successes.map(r => r.getHostname -> r).toMap.asJava)
    when(r.getExceptions).thenReturn(exceptions.asJava)
    val groupedFailures = failures.groupBy(_.getRequest).mapValues(_.asJava)
    when(r.getFailures).thenReturn(groupedFailures.asJava)
    when(r.getLeasesAdded).thenReturn(leasesAdded)
    when(r.getLeasesRejected).thenReturn(leasesRejected)
    when(r.getRuntime).thenReturn(0)
    when(r.getNumAllocations).thenThrow(new NotImplementedError())
    when(r.getTotalVMsCount).thenThrow(new NotImplementedError())
    when(r.getIdleVMsCount).thenThrow(new NotImplementedError())
    r
  }


  /**
    * Mock a task scheduler.
    * The task assigner/unassigner is pre-wired.
    */
  def taskScheduler() = {
    val optimizer = mock(classOf[TaskScheduler])
    val taskAssigner = mock(classOf[Action2[TaskRequest, String]])
    when[Action2[TaskRequest, String]](optimizer.getTaskAssigner).thenReturn(taskAssigner)
    val taskUnassigner = mock(classOf[Action2[String, String]])
    when[Action2[String, String]](optimizer.getTaskUnAssigner).thenReturn(taskUnassigner)
    optimizer
  }

  /**
    * Create a task scheduler builder.
    */
  def taskSchedulerBuilder(optimizer: TaskScheduler) = new TaskSchedulerBuilder {
    var leaseRejectAction: Action1[VirtualMachineLease] = null
    override def withLeaseRejectAction(
        action: Action1[VirtualMachineLease]): TaskSchedulerBuilder = {
      leaseRejectAction = action
      this
    }
    override def build(): TaskScheduler = optimizer
  }

  /**
    * Process a call to scheduleOnce with the given function.
    */
  def scheduleOnce(f: (Seq[TaskRequest],Seq[VirtualMachineLease]) => SchedulingResult) = {
    new Answer[SchedulingResult] {
      override def answer(invocationOnMock: InvocationOnMock): SchedulingResult = {
        val args = invocationOnMock.getArguments
        val requests = args(0).asInstanceOf[java.util.List[TaskRequest]]
        val newLeases = args(1).asInstanceOf[java.util.List[VirtualMachineLease]]
        f(requests.asScala, newLeases.asScala)
      }
    }
  }

  /**
    * The context fixture.
    */
  class Context {
    val optimizer = taskScheduler()
    val optimizerBuilder = taskSchedulerBuilder(optimizer)
    val schedulerDriver = mock(classOf[SchedulerDriver])
    val trace = Mockito.inOrder(schedulerDriver)
    val fsm =
      TestFSMRef(new LaunchCoordinator(testActor, config, schedulerDriver, optimizerBuilder))

    val framework = randomFramework
    val task1 = randomTask
    val task2 = randomTask
    val task3 = randomTask

    val slave1 = {
      val slave = randomSlave
      (slave._1, slave._2,
        randomOffer(framework, slave), randomOffer(framework, slave), randomOffer(framework, slave))
    }

    val slave2 = {
      val slave = randomSlave
      (slave._1, slave._2,
        randomOffer(framework, slave), randomOffer(framework, slave), randomOffer(framework, slave))
    }
  }

  def inState = afterWord("in state")
  def handle = afterWord("handle")

  def handlesAssignments(state: TaskState) = {
    "Unassign" which {
      s"stays in $state with updated optimizer state" in new Context {
        optimizer.getTaskAssigner.call(task1._2.taskRequest, slave1._2)
        fsm.setState(state)
        fsm ! Unassign(task1._1, slave1._2)
        verify(optimizer.getTaskUnAssigner).call(task1._1.getValue, slave1._2)
        fsm.stateName should be (state)
      }
    }
    "Assign" which {
      s"stays in $state with updated optimizer state" in new Context {
        fsm.setState(state)
        fsm ! Assign(Seq(new FlinkTuple2(task1._2.taskRequest, slave1._2)).asJava)
        verify(optimizer.getTaskAssigner).call(MM.any(), MM.any())
        fsm.stateName should be (state)
      }
    }
  }

  "The LaunchCoordinator" when inState {

    "Suspended" should handle {
      "Connected" which {
        "transitions to Idle when the task queue is empty" in new Context {
          fsm.setState(Suspended)
          fsm ! new Connected {}
          fsm.stateName should be (Idle)
        }
        "transitions to GatheringOffers when the task queue is non-empty" in new Context {
          fsm.setState(Suspended, GatherData(tasks = Seq(task1._2), newLeases = Nil))
          fsm ! new Connected {}
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2)
        }
      }
      "Launch" which {
        "stays in Suspended with updated task queue" in new Context {
          fsm.setState(Suspended, GatherData(tasks = Seq(task1._2), newLeases = Nil))
          fsm ! Launch(Seq(task2._2).asJava)
          fsm.stateName should be (Suspended)
          fsm.stateData.tasks should contain only (task1._2, task2._2)
        }
      }

      behave like handlesAssignments(Suspended)
    }

    "Idle" should handle {
      "Disconnected" which {
        "transitions to Suspended" in new Context {
          fsm.setState(Idle)
          fsm ! new Disconnected()
          fsm.stateName should be (Suspended)
        }
      }
      "ResourceOffers" which {
        "stays in Idle with offers declined" in new Context {
          fsm.setState(Idle)
          fsm ! new ResourceOffers(Seq(slave1._3, slave1._4).asJava)
          verify(schedulerDriver).declineOffer(slave1._3.getId)
          verify(schedulerDriver).declineOffer(slave1._4.getId)
          fsm.stateName should be (Idle)
        }
      }
      "Launch" which {
        "transitions to GatheringOffers with updated task queue" in new Context {
          fsm.setState(Idle)
          fsm ! Launch(Seq(task1._2, task2._2).asJava)
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2, task2._2)
        }
      }

      behave like handlesAssignments(Idle)
    }

    "GatheringOffers" should handle {
      "(enter)" which {
        "revives offers" in new Context {
          fsm.setState(GatheringOffers, GatherData())
          verify(schedulerDriver).reviveOffers()
        }
      }
      "(exit)" which {
        "suppresses offers" in new Context {
          fsm.setState(GatheringOffers, GatherData())
          fsm ! new Disconnected()
          verify(schedulerDriver).suppressOffers()
        }
        "declines any outstanding offers" in new Context {
          fsm.setState(GatheringOffers, GatherData())
          fsm ! new Disconnected()
          verify(optimizer).expireAllLeases()
          verify(optimizer).scheduleOnce(MM.any(), MM.any())
        }
      }
      "Disconnected" which {
        "transitions to Suspended with task queue intact" in new Context {
          fsm.setState(GatheringOffers, GatherData(tasks = Seq(task1._2)))
          fsm ! new Disconnected()
          fsm.stateName should be (Suspended)
          fsm.stateData.tasks should contain only (task1._2)
        }
        "transitions to Suspended with offer queue emptied" in new Context {
          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! new Disconnected()
          fsm.stateName should be (Suspended)
          fsm.stateData.newLeases should be (empty)
        }
      }
      "Launch" which {
        "stays in GatheringOffers with updated task queue" in new Context {
          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! Launch(Seq(task2._2).asJava)
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2, task2._2)
          fsm.stateData.newLeases.map(_.getOffer) should contain only (slave1._3)
        }
      }
      "ResourceOffers" which {
        "stays in GatheringOffers with offer queue updated" in new Context {
          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! new ResourceOffers(Seq(slave1._4, slave2._3).asJava)
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2)
          fsm.stateData.newLeases.map(_.getOffer) should contain only
            (slave1._3, slave1._4, slave2._3)
        }
      }
      "OfferRescinded" which {
        "stays in GatheringOffers with offer queue updated" in new Context {
          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! new OfferRescinded(slave1._3.getId)
          verify(optimizer).expireLease(slave1._3.getId.getValue)
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2)
          fsm.stateData.newLeases should be (empty)
        }
      }
      "StateTimeout" which {
        "sends AcceptOffers message for matched tasks" in new Context {
          when(optimizer.scheduleOnce(MM.any(), MM.any())) thenAnswer {
            scheduleOnce { (requests, newLeases) =>
              val (l, task) = (newLeases.head, requests.head)
              val vm = vmAssignmentResult(l.hostname(), Seq(l), Set(taskAssignmentResult(l, task)))
              schedulingResult(successes = Seq(vm))
            }
          } thenReturn(schedulingResult(successes = Nil))

          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! StateTimeout
          val offers = expectMsgType[AcceptOffers]
          offers.hostname() should be (slave1._2)
          offers.offerIds() should contain only (slave1._3.getId)
        }
        "transitions to Idle when task queue is empty" in new Context {
          when(optimizer.scheduleOnce(MM.any(), MM.any())) thenAnswer {
            scheduleOnce { (requests, newLeases) =>
              val (l, task) = (newLeases.head, requests.head)
              val vm = vmAssignmentResult(l.hostname(), Seq(l), Set(taskAssignmentResult(l, task)))
              schedulingResult(successes = Seq(vm))
            }
          } thenReturn(schedulingResult(successes = Nil))

          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! StateTimeout
          fsm.stateName should be (Idle)
          fsm.stateData.tasks should be (empty)
          fsm.stateData.newLeases should be (empty)
        }
        "stays in GatheringOffers when task queue is non-empty" in new Context {
          when(optimizer.scheduleOnce(MM.any(), MM.any())) thenAnswer {
            scheduleOnce { (requests, newLeases) =>
              schedulingResult(successes = Nil)
            }
          }
          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! StateTimeout
          fsm.stateName should be (GatheringOffers)
          fsm.stateData.tasks should contain only (task1._2)
          fsm.stateData.newLeases should be (empty)
        }
        "declines old offers" in new Context {
          when(optimizer.scheduleOnce(MM.any(), MM.any())) thenAnswer {
            scheduleOnce { (requests, newLeases) =>
              optimizerBuilder.leaseRejectAction.call(newLeases.head)
              schedulingResult(successes = Nil)
            }
          } thenReturn(schedulingResult(successes = Nil))

          fsm.setState(GatheringOffers,
            GatherData(tasks = Seq(task1._2), newLeases = Seq(lease(slave1._3))))
          fsm ! StateTimeout
          verify(schedulerDriver).declineOffer(slave1._3.getId)
        }
      }

      behave like handlesAssignments(GatheringOffers)
    }
  }

  override def toString = s"LaunchCoordinatorTest()"
}
