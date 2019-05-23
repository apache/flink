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

import akka.actor.ActorSystem
import akka.actor.FSM.StateTimeout
import akka.testkit._
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.TestFSMUtils
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator.Reconcile
import org.apache.flink.mesos.scheduler.messages.{Connected, Disconnected, StatusUpdate}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.mesos.{Protos, SchedulerDriver}
import org.apache.mesos.Protos.TaskState._
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class TaskMonitorTest
  extends TestKitBase
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import TaskMonitor._

  lazy val config: Configuration = new Configuration()
  implicit lazy val system: ActorSystem = AkkaUtils.createLocalActorSystem(config)

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
      .setTaskId(taskID).setSlaveId(slaveID).setState(TASK_STAGING).build()
    (taskID, taskStatus)
  }

  class Context {
    val schedulerDriver = mock(classOf[SchedulerDriver])
    val trace = Mockito.inOrder(schedulerDriver)
    val slave = randomSlave
    val task = randomTask(slave._1)
    val parent = TestProbe()
    val fsm =
      TestFSMUtils.testFSMRef(new TaskMonitor(config, schedulerDriver, New(task._1)), parent.ref)
    parent.watch(fsm)
  }

  def inState = afterWord("in state")
  def handle = afterWord("handle")

  def handlesStatusUpdate(state: TaskMonitorState) = {
    "StatusUpdate" which {
      "transitions to Staging when goal state is Launched and status is staging" in new Context {
        fsm.setState(state, StateData(Launched(task._1, slave._1)))
        fsm ! new StatusUpdate(task._2.toBuilder.setState(TASK_STAGING).build())
        fsm.stateName should be (Staging)
        fsm.stateData should be (StateData(Launched(task._1, slave._1)))
      }
      "transitions to Running when goal state is Launched and status is running" in new Context {
        fsm.setState(state, StateData(Launched(task._1, slave._1)))
        fsm ! new StatusUpdate(task._2.toBuilder.setState(TASK_RUNNING).build())
        fsm.stateName should be (Running)
        fsm.stateData should be (StateData(Launched(task._1, slave._1)))
      }
      "stops when goal state is Launched and status is TASK_KILLED" in new Context {
        fsm.setState(state, StateData(Launched(task._1, slave._1)))
        fsm ! new StatusUpdate(task._2.toBuilder.setState(TASK_KILLED).build())
        parent.fishForMessage() {
          case msg: Reconcile => false // initial state might have been Reconciling
          case msg: TaskTerminated => true
        }
        parent.expectTerminated(fsm)
      }
      "transitions to Killing when goal state is Released and status is running" in new Context {
        fsm.setState(state, StateData(Released(task._1, slave._1)))
        fsm ! new StatusUpdate(task._2.toBuilder.setState(TASK_RUNNING).build())
        fsm.stateName should be (Killing)
        fsm.stateData should be (StateData(Released(task._1, slave._1)))
      }
      "stops when goal state is Released and status is killed" in new Context {
        fsm.setState(state, StateData(Released(task._1, slave._1)))
        fsm ! new StatusUpdate(task._2.toBuilder.setState(TASK_KILLED).build())
        parent.fishForMessage() {
          case msg: Reconcile => false // initial state might have been Reconciling
          case msg: TaskTerminated => true
        }
        parent.expectTerminated(fsm)
      }
    }
  }

  def handlesDisconnect(state: TaskMonitorState) = {
    "Disconnected" which {
      "transitions to Suspended" in new Context {
        fsm.setState(state, StateData(New(task._1)))
        fsm ! new Disconnected()
        fsm.stateName should be (Suspended)
        fsm.stateData should be (StateData(New(task._1)))
      }
    }
  }

  def handlesRelease(state: TaskMonitorState) = {
    "TaskGoalStateUpdated" which {
      "transitions to Killing when the new goal state is Released" in new Context {
        fsm.setState(state, StateData(Launched(task._1, slave._1)))
        fsm ! TaskGoalStateUpdated(Released(task._1, slave._1))
        fsm.stateName should be (Killing)
        fsm.stateData.goal should be (Released(task._1, slave._1))
      }
    }
  }

  "TaskMonitor" when inState {

    "Suspended" should handle {
      "TaskGoalStateUpdated" which {
        "stays in Suspended with updated state data" in new Context {
          fsm.setState(Suspended, StateData(Launched(task._1, slave._1)))
          fsm ! TaskGoalStateUpdated(Released(task._1, slave._1))
          fsm.stateName should be (Suspended)
          fsm.stateData.goal should be (Released(task._1, slave._1))
        }
      }
      "StatusUpdate" which {
        "is disregarded" in new Context {
          fsm.setState(Suspended, StateData(Launched(task._1, slave._1)))
          fsm ! new StatusUpdate(task._2)
          fsm.stateName should be (Suspended)
        }
      }
      "Connected" which {
        "transitions to New when the goal state is New" in new Context {
          fsm.setState(Suspended, StateData(New(task._1)))
          fsm ! new Connected() {}
          fsm.stateName should be (New)
          fsm.stateData should be (StateData(New(task._1)))
        }
        "transitions to Reconciling when the goal state is Launched" in new Context {
          fsm.setState(Suspended, StateData(Launched(task._1, slave._1)))
          fsm ! new Connected() {}
          fsm.stateName should be (Reconciling)
          fsm.stateData should be (StateData(Launched(task._1, slave._1)))
        }
        "transitions to Killing when the goal state is Released" in new Context {
          fsm.setState(Suspended, StateData(Released(task._1, slave._1)))
          fsm ! new Connected() {}
          fsm.stateName should be (Killing)
          fsm.stateData should be (StateData(Released(task._1, slave._1)))
        }
      }
    }

    "New" should handle {
      "TaskGoalStateUpdated" which {
        "transitions to Staging when the new goal state is Launched" in new Context {
          fsm.setState(New, StateData(New(task._1)))
          fsm ! TaskGoalStateUpdated(Launched(task._1, slave._1))
          fsm.stateName should be (Staging)
          fsm.stateData should be (StateData(Launched(task._1, slave._1)))
        }
      }
      behave like handlesDisconnect(New)
    }

    "Reconciling" should handle {
      "(enter)" which {
        "initiates reconciliation when goal state is Launched" in new Context {
          fsm.setState(Reconciling, StateData(Launched(task._1, slave._1)))
          parent.expectMsgType[Reconcile]
        }
      }
      behave like handlesStatusUpdate(Reconciling)
      behave like handlesDisconnect(Reconciling)
      behave like handlesRelease(Reconciling)
    }

    "Staging" should handle {
      "StateTimeout" which {
        "transitions to Reconciling" in new Context {
          fsm.setState(Staging, StateData(Launched(task._1, slave._1)))
          fsm ! StateTimeout
          fsm.stateName should be (Reconciling)
          fsm.stateData should be (StateData(Launched(task._1, slave._1)))
        }
      }
      behave like handlesStatusUpdate(Staging)
      behave like handlesDisconnect(Staging)
      behave like handlesRelease(Staging)
    }

    "Running" should handle {
      behave like handlesStatusUpdate(Running)
      behave like handlesDisconnect(Running)
      behave like handlesRelease(Running)
    }

    "Killing" should handle {
      "(enter)" which {
        "sends kill message to Mesos" in new Context {
          fsm.setState(Killing, StateData(Released(task._1, slave._1)))
          verify(schedulerDriver).killTask(task._1)
        }
      }
      "TaskGoalStateUpdated" which {
        "stays in Killing when the new goal state is Released" in new Context {
          fsm.setState(Killing, StateData(Launched(task._1, slave._1)))
          fsm ! TaskGoalStateUpdated(Released(task._1, slave._1))
          fsm.stateName should be (Killing)
          fsm.stateData.goal should be (Released(task._1, slave._1))
        }
      }
      "StateTimeout" which {
        "stays in Killing with another kill message sent" in new Context {
          fsm.setState(Killing, StateData(Released(task._1, slave._1)))
          reset(schedulerDriver)
          fsm ! StateTimeout
          verify(schedulerDriver).killTask(task._1)
          fsm.stateName should be (Killing)
        }
      }
      behave like handlesStatusUpdate(Killing)
      behave like handlesDisconnect(Killing)
    }
  }
}
