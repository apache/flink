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
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManager;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * This class reacts to callbacks from the Mesos scheduler driver.
 *
 * Forwards incoming messages to the {@link MesosResourceManager} RPC gateway.
 *
 * See https://mesos.apache.org/api/latest/java/org/apache/mesos/Scheduler.html
 */
public class SchedulerProxyV2 implements Scheduler {

	/** The actor to which we report the callbacks */
	private final SchedulerGateway gateway;

	public SchedulerProxyV2(SchedulerGateway gateway) {
		this.gateway = gateway;
	}

	@Override
	public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
		gateway.registered(new Registered(frameworkId, masterInfo));
	}

	@Override
	public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
		gateway.reregistered(new ReRegistered(masterInfo));
	}

	@Override
	public void disconnected(SchedulerDriver driver) {
		gateway.disconnected(new Disconnected());
	}

	@Override
	public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
		gateway.resourceOffers(new ResourceOffers(offers));
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
		gateway.offerRescinded(new OfferRescinded(offerId));
	}

	@Override
	public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
		gateway.statusUpdate(new StatusUpdate(status));
	}

	@Override
	public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
		gateway.frameworkMessage(new FrameworkMessage(executorId, slaveId, data));
	}

	@Override
	public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
		gateway.slaveLost(new SlaveLost(slaveId));
	}

	@Override
	public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
		gateway.executorLost(new ExecutorLost(executorId, slaveId, status));
	}

	@Override
	public void error(SchedulerDriver driver, String message) {
		gateway.error(new Error(message));
	}
}
