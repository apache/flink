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

import java.util.UUID

import akka.actor._
import akka.testkit._
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.TestFSMUtils
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator.Reconcile
import org.apache.flink.mesos.scheduler.TaskMonitor._
import org.apache.flink.mesos.scheduler.messages.{Connected, Disconnected, StatusUpdate}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.{Protos, SchedulerDriver}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable.{Map => MutableMap}

@RunWith(classOf[JUnitRunner])
class TasksTest
    extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  lazy val config = new Configuration()
  implicit lazy val system = AkkaUtils.createLocalActorSystem(config)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def randomSlave = {
    val slaveID = Protos.SlaveID.newBuilder.setValue(UUID.randomUUID.toString).build
    val hostname = s"host-${slaveID.getValue}"
    (slaveID, hostname)
  }

  def randomTask(slaveID: Protos.SlaveID) = {
    val taskID = Protos.TaskID.newBuilder.setValue(UUID.randomUUID.toString).build
    val taskStatus = Protos.TaskStatus.newBuilder()
      .setTaskId(taskID).setSlaveId(slaveID).setState(TASK_STAGING)
    (taskID, taskStatus)
  }

  def childProbe(parent: ActorRefFactory): (TestProbe, ActorRef) = {
    val probe = TestProbe()
    val childRef = parent.actorOf(Props(
      new Actor {
        override def receive: Receive = {
          case msg @ _ => probe.ref.forward(msg)
        }
      }
    ))
    (probe,childRef)
  }

  class Context(implicit val system: ActorSystem) extends TestKitBase with ImplicitSender {

    case class MockTaskMonitor(probe: TestProbe, actorRef: ActorRef, task: TaskGoalState)

    val schedulerDriver = mock(classOf[SchedulerDriver])

    val slave = randomSlave
    val task = randomTask(slave._1)

    val taskActors = MutableMap[Protos.TaskID,MockTaskMonitor]()

    val actor = {
      val taskActorCreator = (factory: ActorRefFactory, task: TaskGoalState) => {
        val (probe, taskActorRef) = childProbe(factory)
        taskActors.put(task.taskID, MockTaskMonitor(probe, taskActorRef, task))
        taskActorRef
      }
      TestActorRef[Tasks](
        Props(classOf[Tasks], testActor, config, schedulerDriver, taskActorCreator),
        testActor,
        TestFSMUtils.randomName)
    }
  }

  def handle = afterWord("handle")

  "Tasks" should handle {

    "(supervision)" which {
      "escalates" in new Context {
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        watch(actor)
        taskActors(task._1).actorRef ! Kill
        expectTerminated(actor)
      }
    }

    "Connect" which {
      "stores the connected message for later use" in new Context {
        val msg = new Connected() {}
        actor ! msg
        actor.underlyingActor.registered should be (Some(msg))
      }

      "forwards the message to child tasks" in new Context {
        val msg = new Connected() {}
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        actor ! msg
        taskActors(task._1).probe.expectMsg(msg)
      }
    }

    "Disconnect" which {
      "releases any connected message that was previously stored" in new Context {
        actor.underlyingActor.registered = Some(new Connected() {})
        actor ! new Disconnected()
        actor.underlyingActor.registered should be (None)
      }

      "forwards the message to child tasks" in new Context {
        val msg = new Disconnected() {}
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        actor ! msg
        taskActors(task._1).probe.expectMsg(msg)
      }
    }

    "TaskGoalStateUpdated" which {
      "creates a task monitor on-demand for a given task" in new Context {
        val goal = Launched(task._1, slave._1)
        actor ! TaskGoalStateUpdated(goal)
        actor.underlyingActor.taskMap.contains(task._1) should be (true)
        taskActors(task._1).task should be (goal)
      }

      "forwards the stored connected message to new monitor actors" in new Context {
        val msg = new Connected() {}
        val goal = Launched(task._1, slave._1)
        actor ! msg
        actor ! TaskGoalStateUpdated(goal)
        taskActors(task._1).probe.expectMsg(msg)
      }

      "forwards the goal state to the task monitor" in new Context {
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        val updateMsg = TaskGoalStateUpdated(Released(task._1, slave._1))
        actor ! updateMsg
        taskActors(task._1).probe.expectMsg(updateMsg)
      }
    }

    "StatusUpdate" which {
      "forwards the update to a task monitor" in new Context {
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        val msg = new StatusUpdate(task._2.setState(TASK_RUNNING).build())
        actor ! msg
        taskActors(task._1).probe.expectMsg(msg)
      }

      "resumes monitoring of resurrected tasks" in new Context {
        // in this scenario, no goal state is sent prior to the status update
        actor ! new StatusUpdate(task._2.setState(TASK_RUNNING).build())
        taskActors.contains(task._1) should be (true)
        taskActors(task._1).task should be (Released(task._1, slave._1))
      }
    }

    "Reconcile" which {
      "forwards the message to the parent" in new Context {
        val msg = new Reconcile(Seq(task._2.build()))
        actor ! msg
        expectMsg(msg)
      }
    }

    "TaskTerminated" which {
      "removes the task monitor ref" in new Context {
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        actor.underlyingActor.taskMap.contains(task._1) should be (true)
        actor ! TaskTerminated(task._1, task._2.setState(TASK_FAILED).build())
        actor.underlyingActor.taskMap.contains(task._1) should be (false)
      }

      "forwards to the parent" in new Context {
        actor ! TaskGoalStateUpdated(Launched(task._1, slave._1))
        val msg = TaskTerminated(task._1, task._2.setState(TASK_FAILED).build())
        actor ! msg
        expectMsg(msg)
      }
    }
  }

}
