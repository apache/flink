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

package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.RegisteredTaskManagers;
import org.apache.flink.runtime.messages.JobManagerMessages.TaskManagerInstance;
import org.apache.flink.util.StringUtils;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskManagersHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final FiniteDuration timeout;

	public static final String TASK_MANAGER_ID_KEY = "taskmanagerid";
	
	public TaskManagersHandler(FiniteDuration timeout) {
		this.timeout = checkNotNull(timeout);
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			if (jobManager != null) {
				// whether one task manager's metrics are requested, or all task manager, we
				// return them in an array. This avoids unnecessary code complexity.
				// If only one task manager is requested, we only fetch one task manager metrics.
				final List<Instance> instances = new ArrayList<>();
				if (pathParams.containsKey(TASK_MANAGER_ID_KEY)) {
					try {
						InstanceID instanceID = new InstanceID(StringUtils.hexStringToByte(pathParams.get(TASK_MANAGER_ID_KEY)));
						Future<Object> future = jobManager.ask(new JobManagerMessages.RequestTaskManagerInstance(instanceID), timeout);
						TaskManagerInstance instance = (TaskManagerInstance) Await.result(future, timeout);
						if (instance.instance().nonEmpty()) {
							instances.add(instance.instance().get());
						}
					}
					// this means the id string was invalid. Keep the list empty.
					catch (IllegalArgumentException e){
						// do nothing.
					}
				} else {
					Future<Object> future = jobManager.ask(JobManagerMessages.getRequestRegisteredTaskManagers(), timeout);
					RegisteredTaskManagers taskManagers = (RegisteredTaskManagers) Await.result(future, timeout);
					instances.addAll(taskManagers.asJavaCollection());
				}

				StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

				gen.writeStartObject();
				gen.writeArrayFieldStart("taskmanagers");

				for (Instance instance : instances) {
					gen.writeStartObject();
					gen.writeStringField("id", instance.getId().toString());
					gen.writeStringField("path", instance.getActorGateway().path());
					gen.writeNumberField("dataPort", instance.getInstanceConnectionInfo().dataPort());
					gen.writeNumberField("timeSinceLastHeartbeat", instance.getLastHeartBeat());
					gen.writeNumberField("slotsNumber", instance.getTotalNumberOfSlots());
					gen.writeNumberField("freeSlots", instance.getNumberOfAvailableSlots());
					gen.writeNumberField("cpuCores", instance.getResources().getNumberOfCPUCores());
					gen.writeNumberField("physicalMemory", instance.getResources().getSizeOfPhysicalMemory());
					gen.writeNumberField("freeMemory", instance.getResources().getSizeOfJvmHeap());
					gen.writeNumberField("managedMemory", instance.getResources().getSizeOfManagedMemory());

					// only send metrics when only one task manager requests them.
					if (pathParams.containsKey(TASK_MANAGER_ID_KEY)) {
						byte[] report = instance.getLastMetricsReport();
						if (report != null) {
							gen.writeFieldName("metrics");
							gen.writeRawValue(new String(report, "utf-8"));
						}
					}

					gen.writeEndObject();
				}

				gen.writeEndArray();
				gen.writeEndObject();

				gen.close();
				return writer.toString();
			}
			else {
				throw new Exception("No connection to the leading JobManager.");
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to fetch list of all task managers: " + e.getMessage(), e);
		}
	}
}
