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

import akka.actor.FSM
import akka.testkit._
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.Matchers._
import org.apache.flink.mesos.scheduler.messages.{Connected, Disconnected, StatusUpdate}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.{SchedulerDriver, Protos}
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class ReconciliationCoordinatorTest
    extends TestKitBase
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  import ReconciliationCoordinator._

  lazy val config = new Configuration()
  implicit lazy val system = AkkaUtils.createLocalActorSystem(config)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def randomTask = {
    val slaveID = Protos.SlaveID.newBuilder.setValue(UUID.randomUUID.toString).build()
    val taskID = Protos.TaskID.newBuilder.setValue(UUID.randomUUID.toString).build
    val taskStatus = Protos.TaskStatus.newBuilder()
      .setTaskId(taskID).setSlaveId(slaveID).setState(TASK_STAGING).build()
    (taskID, taskStatus)
  }

  class Context {
    val schedulerDriver = mock(classOf[SchedulerDriver])
    val trace = Mockito.inOrder(schedulerDriver)
    val fsm = TestFSMRef(new ReconciliationCoordinator(config, schedulerDriver))
    val task1 = randomTask
    val task2 = randomTask
    val task3 = randomTask
  }

  def inState = afterWord("in state")
  def handle = afterWord("handle")

  "The ReconciliationCoordinator" when inState {

    "Suspended" should handle {
      "Connected" which {
        "transitions to Idle when the queue is empty" in new Context {
          fsm.setState(Suspended)
          fsm ! new Connected {}
          fsm.stateName should be (Idle)
          fsm.stateData.remaining should be (empty)
        }

        "transitions to Reconciling when the queue is non-empty" in new Context {
          fsm.setState(Suspended, ReconciliationData(Seq(randomTask).toMap))
          fsm ! new Connected {}
          fsm.stateName should be (Reconciling)
        }
      }

      "Reconcile" which {
        "queues requests" in new Context {
          fsm.setState(Suspended)
          fsm ! new Reconcile(Seq(task1).unzip._2)
          fsm.stateData.remaining should contain only (task1)
        }
      }

      "StatusUpdate" which {
        "is disregarded" in new Context {
          fsm.setState(Suspended)
          fsm ! new StatusUpdate(task1._2)
          fsm.stateName should be (Suspended)
        }
      }
    }

    "Idle" should handle {

      "Disconnected" which {
        "transitions to Suspended" in new Context {
          fsm ! new Disconnected()
          fsm.stateName should be (Suspended)
        }
      }

      "Reconcile" which {
        "transitions to Reconciling with the given tasks" in new Context {
          fsm.setState(Idle)
          fsm ! new Reconcile(Seq(task1._2))

          verify(schedulerDriver).reconcileTasks(contentsMatch(Seq(task1._2)))
          fsm.stateName should be (Reconciling)
          fsm.stateData.remaining should contain only (task1)
        }
      }

      "StatusUpdate" which {
        "is disregarded" in new Context {
          fsm.setState(Idle)
          fsm ! new StatusUpdate(task1._2)
          fsm.stateName should be (Idle)
          fsm.stateData.remaining should be (empty)
        }
      }
    }

    "Reconciling" should handle {

      "(enter)" which {
        "initiates reconciliation" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1).toMap))
          verify(schedulerDriver).reconcileTasks(contentsMatch(Seq(task1._2)))
        }
      }

      "Disconnected" which {
        "transitions to Suspended with queue intact" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1).toMap))
          reset(schedulerDriver)
          fsm ! FSM.StateTimeout
          fsm ! new Disconnected()

          fsm.stateName should be (Suspended)
          fsm.stateData.remaining should contain only (task1)
        }

        "transitions to Suspended with retry counter reset" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1).toMap))
          reset(schedulerDriver)
          fsm ! FSM.StateTimeout
          fsm.stateData.retries should be (1)
          fsm ! new Disconnected()

          fsm.stateName should be (Suspended)
          fsm.stateData.retries should be (0)
        }
      }

      "Reconcile" which {
        "stays in Reconciling with additional tasks" in new Context {
          // testing the aggregation logic of: (A,B)+(B,C)=(A,B,C)
          fsm.setState(Reconciling, ReconciliationData(Seq(task1, task2).toMap))
          reset(schedulerDriver)
          fsm ! new Reconcile(Seq(task2, task3).unzip._2)

          // the reconcileTasks message should mention only the new tasks
          verify(schedulerDriver).reconcileTasks(contentsMatch(Seq(task2, task3).unzip._2))
          fsm.stateName should be (Reconciling)
          fsm.stateData.remaining should contain only (task1, task2, task3)
        }
      }

      "StatusUpdate" which {
        "transitions to Idle when queue is empty" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1).toMap))
          reset(schedulerDriver)
          val taskStatus = task1._2.toBuilder.setState(TASK_LOST).build()
          fsm ! FSM.StateTimeout
          fsm ! new StatusUpdate(taskStatus)

          fsm.stateName should be (Idle)
          fsm.stateData.remaining should be (empty)
          fsm.stateData.retries should be (0)
        }

        "stays in Reconciling when queue is non-empty" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1, task2).toMap))
          reset(schedulerDriver)
          val taskStatus = task1._2.toBuilder.setState(TASK_LOST).build()
          fsm ! new StatusUpdate(taskStatus)

          fsm.stateName should be (Reconciling)
          fsm.stateData.remaining should contain only (task2)
        }

        "ignores updates for unrecognized tasks" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1).toMap))
          reset(schedulerDriver)
          val taskStatus = task2._2.toBuilder.setState(TASK_LOST).build()
          fsm ! new StatusUpdate(taskStatus)

          fsm.stateName should be (Reconciling)
          fsm.stateData.remaining should contain only (task1)
        }
      }

      "StateTimeout" which {
        "stays in Reconciling with reconciliation retry" in new Context {
          fsm.setState(Reconciling, ReconciliationData(Seq(task1,task2).toMap))
          reset(schedulerDriver)
          fsm ! FSM.StateTimeout

          verify(schedulerDriver).reconcileTasks(contentsMatch(Seq(task1,task2).unzip._2))
          fsm.stateName should be (Reconciling)
          fsm.stateData.remaining should contain only (task1,task2)
          fsm.stateData.retries should be (1)
        }
      }
    }
  }
}
