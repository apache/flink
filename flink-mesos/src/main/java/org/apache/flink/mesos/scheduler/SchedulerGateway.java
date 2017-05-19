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
import org.apache.flink.runtime.rpc.RpcGateway;

/**
 * A scheduler's RPC gateway interface.
 *
 * Implemented by RPC endpoints that accept Mesos scheduler messages.
 */
public interface SchedulerGateway extends RpcGateway {

	/**
	 * Called when connected to Mesos as a new framework.
	 */
	void registered(Registered message);

	/**
	 * Called when reconnected to Mesos following a failover event.
	 */
	void reregistered(ReRegistered message);

	/**
	 * Called when disconnected from Mesos.
	 */
	void disconnected(Disconnected message);

	/**
	 * Called when resource offers are made to the framework.
	 */
	void resourceOffers(ResourceOffers message);

	/**
	 * Called when resource offers are rescinded.
	 */
	void offerRescinded(OfferRescinded message);

	/**
	 * Called when a status update arrives from the Mesos master.
	 */
	void statusUpdate(StatusUpdate message);

	/**
	 * Called when a framework message arrives from a custom Mesos task executor.
	 */
	void frameworkMessage(FrameworkMessage message);

	/**
	 * Called when a Mesos slave is lost.
	 */
	void slaveLost(SlaveLost message);

	/**
	 * Called when a custom Mesos task executor is lost.
	 */
	void executorLost(ExecutorLost message);

	/**
	 * Called when an error is reported by the scheduler callback.
	 */
	void error(Error message);
}
