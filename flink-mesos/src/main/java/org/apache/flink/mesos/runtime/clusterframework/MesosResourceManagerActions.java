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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;

/**
 * Actions defined by the MesosResourceManager.
 *
 * <p>These are called by the MesosResourceManager components such
 * as {@link LaunchCoordinator}, and {@link TaskMonitor}.
 */
public interface MesosResourceManagerActions {

	/**
	 * Accept the given offers as advised by the launch coordinator.
	 *
	 * <p>Note: This method is a callback for the {@link LaunchCoordinator}.
	 *
	 * @param offersToAccept Offers to accept from Mesos
	 */
	void acceptOffers(AcceptOffers offersToAccept);

	/**
	 * Trigger reconciliation with the Mesos master.
	 *
	 * <p>Note: This method is a callback for the {@link TaskMonitor}.
	 *
	 * @param reconciliationRequest Message containing the tasks which shall be reconciled
	 */
	void reconcile(ReconciliationCoordinator.Reconcile reconciliationRequest);

	/**
	 * Notify that the given Mesos task has been terminated.
	 *
	 * <p>Note: This method is a callback for the {@link TaskMonitor}.
	 *
	 * @param terminatedTask Message containing the terminated task
	 */
	void taskTerminated(TaskMonitor.TaskTerminated terminatedTask);
}
