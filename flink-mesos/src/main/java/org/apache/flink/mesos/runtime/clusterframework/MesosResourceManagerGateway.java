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

import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.SchedulerGateway;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

/**
 * The {@link MesosResourceManager}'s RPC gateway interface.
 */
public interface MesosResourceManagerGateway extends ResourceManagerGateway, SchedulerGateway {

	void acceptOffers(AcceptOffers msg);

	void reconcile(ReconciliationCoordinator.Reconcile message);

	void taskTerminated(TaskMonitor.TaskTerminated message);
}
