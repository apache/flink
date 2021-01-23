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

package org.apache.flink.mesos.scheduler;

import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.ExecutorLost;
import org.apache.flink.mesos.scheduler.messages.FrameworkMessage;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.SlaveLost;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;

import akka.actor.ActorRef;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * This class reacts to callbacks from the Mesos scheduler driver.
 *
 * <p>In order to preserve actor concurrency safety, this class simply sends corresponding messages
 * to the Mesos resource master actor.
 *
 * <p>See https://mesos.apache.org/api/latest/java/org/apache/mesos/Scheduler.html
 */
public class SchedulerProxy implements Scheduler {

    /** The actor to which we report the callbacks. */
    private final ActorRef mesosActor;

    public SchedulerProxy(ActorRef mesosActor) {
        this.mesosActor = mesosActor;
    }

    @Override
    public void registered(
            SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        mesosActor.tell(new Registered(frameworkId, masterInfo), ActorRef.noSender());
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        mesosActor.tell(new ReRegistered(masterInfo), ActorRef.noSender());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        mesosActor.tell(new Disconnected(), ActorRef.noSender());
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        mesosActor.tell(new ResourceOffers(offers), ActorRef.noSender());
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        mesosActor.tell(new OfferRescinded(offerId), ActorRef.noSender());
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        mesosActor.tell(new StatusUpdate(status), ActorRef.noSender());
    }

    @Override
    public void frameworkMessage(
            SchedulerDriver driver,
            Protos.ExecutorID executorId,
            Protos.SlaveID slaveId,
            byte[] data) {
        mesosActor.tell(new FrameworkMessage(executorId, slaveId, data), ActorRef.noSender());
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        mesosActor.tell(new SlaveLost(slaveId), ActorRef.noSender());
    }

    @Override
    public void executorLost(
            SchedulerDriver driver,
            Protos.ExecutorID executorId,
            Protos.SlaveID slaveId,
            int status) {
        mesosActor.tell(new ExecutorLost(executorId, slaveId, status), ActorRef.noSender());
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        mesosActor.tell(new Error(message), ActorRef.noSender());
    }
}
