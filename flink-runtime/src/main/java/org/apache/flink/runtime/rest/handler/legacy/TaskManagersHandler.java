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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * A request handler that provides an overview over all taskmanagers or details for a single one.
 */
public class TaskManagersHandler extends AbstractJsonRequestHandler  {

	private static final String TASKMANAGERS_REST_PATH = "/taskmanagers";
	private static final String TASKMANAGER_DETAILS_REST_PATH = "/taskmanagers/:taskmanagerid";

	public static final String TASK_MANAGER_ID_KEY = "taskmanagerid";

	private final Time timeout;

	private final MetricFetcher fetcher;

	public TaskManagersHandler(Executor executor, Time timeout, MetricFetcher fetcher) {
		super(executor);
		this.timeout = requireNonNull(timeout);
		this.fetcher = fetcher;
	}

	@Override
	public String[] getPaths() {
		return new String[]{TASKMANAGERS_REST_PATH, TASKMANAGER_DETAILS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		if (jobManagerGateway != null) {
			// whether one task manager's metrics are requested, or all task manager, we
			// return them in an array. This avoids unnecessary code complexity.
			// If only one task manager is requested, we only fetch one task manager metrics.
			if (pathParams.containsKey(TASK_MANAGER_ID_KEY)) {
				InstanceID instanceID = new InstanceID(StringUtils.hexStringToByte(pathParams.get(TASK_MANAGER_ID_KEY)));
				CompletableFuture<Optional<Instance>> tmInstanceFuture = jobManagerGateway.requestTaskManagerInstance(instanceID, timeout);

				return tmInstanceFuture.thenApplyAsync(
					(Optional<Instance> optTaskManager) -> {
						try {
							return writeTaskManagersJson(
								optTaskManager.map(Collections::singleton).orElse(Collections.emptySet()),
								pathParams);
						} catch (IOException e) {
							throw new CompletionException(new FlinkException("Could not write TaskManagers JSON.", e));
						}
					},
					executor);
			} else {
				CompletableFuture<Collection<Instance>> tmInstancesFuture = jobManagerGateway.requestTaskManagerInstances(timeout);

				return tmInstancesFuture.thenApplyAsync(
					(Collection<Instance> taskManagers) -> {
						try {
							return writeTaskManagersJson(taskManagers, pathParams);
						} catch (IOException e) {
							throw new CompletionException(new FlinkException("Could not write TaskManagers JSON.", e));
						}
					},
					executor);
			}
		}
		else {
			return FutureUtils.completedExceptionally(new Exception("No connection to the leading JobManager."));
		}
	}

	private String writeTaskManagersJson(Collection<Instance> instances, Map<String, String> pathParams) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		gen.writeArrayFieldStart("taskmanagers");

		for (Instance instance : instances) {
			gen.writeStartObject();
			gen.writeStringField("id", instance.getId().toString());
			gen.writeStringField("path", instance.getTaskManagerGateway().getAddress());
			gen.writeNumberField("dataPort", instance.getTaskManagerLocation().dataPort());
			gen.writeNumberField("timeSinceLastHeartbeat", instance.getLastHeartBeat());
			gen.writeNumberField("slotsNumber", instance.getTotalNumberOfSlots());
			gen.writeNumberField("freeSlots", instance.getNumberOfAvailableSlots());
			gen.writeNumberField("cpuCores", instance.getResources().getNumberOfCPUCores());
			gen.writeNumberField("physicalMemory", instance.getResources().getSizeOfPhysicalMemory());
			gen.writeNumberField("freeMemory", instance.getResources().getSizeOfJvmHeap());
			gen.writeNumberField("managedMemory", instance.getResources().getSizeOfManagedMemory());

			// only send metrics when only one task manager requests them.
			if (pathParams.containsKey(TASK_MANAGER_ID_KEY)) {
				fetcher.update();
				final MetricStore metricStore = fetcher.getMetricStore();

				synchronized (metricStore) {
					MetricStore.TaskManagerMetricStore metrics = metricStore.getTaskManagerMetricStore(instance.getId().toString());
					if (metrics != null) {
						gen.writeObjectFieldStart("metrics");
						long heapUsed = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Heap.Used", "0"));
						long heapCommitted = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Heap.Committed", "0"));
						long heapTotal = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Heap.Max", "0"));

						gen.writeNumberField("heapCommitted", heapCommitted);
						gen.writeNumberField("heapUsed", heapUsed);
						gen.writeNumberField("heapMax", heapTotal);

						long nonHeapUsed = Long.valueOf(metrics.getMetric("Status.JVM.Memory.NonHeap.Used", "0"));
						long nonHeapCommitted = Long.valueOf(metrics.getMetric("Status.JVM.Memory.NonHeap.Committed", "0"));
						long nonHeapTotal = Long.valueOf(metrics.getMetric("Status.JVM.Memory.NonHeap.Max", "0"));

						gen.writeNumberField("nonHeapCommitted", nonHeapCommitted);
						gen.writeNumberField("nonHeapUsed", nonHeapUsed);
						gen.writeNumberField("nonHeapMax", nonHeapTotal);

						gen.writeNumberField("totalCommitted", heapCommitted + nonHeapCommitted);
						gen.writeNumberField("totalUsed", heapUsed + nonHeapUsed);
						gen.writeNumberField("totalMax", heapTotal + nonHeapTotal);

						long directCount = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Direct.Count", "0"));
						long directUsed = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Direct.MemoryUsed", "0"));
						long directMax = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Direct.TotalCapacity", "0"));

						gen.writeNumberField("directCount", directCount);
						gen.writeNumberField("directUsed", directUsed);
						gen.writeNumberField("directMax", directMax);

						long mappedCount = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Mapped.Count", "0"));
						long mappedUsed = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Mapped.MemoryUsed", "0"));
						long mappedMax = Long.valueOf(metrics.getMetric("Status.JVM.Memory.Mapped.TotalCapacity", "0"));

						gen.writeNumberField("mappedCount", mappedCount);
						gen.writeNumberField("mappedUsed", mappedUsed);
						gen.writeNumberField("mappedMax", mappedMax);

						long memorySegmentsAvailable = Long.valueOf(metrics.getMetric("Status.Network.AvailableMemorySegments", "0"));
						long memorySegmentsTotal = Long.valueOf(metrics.getMetric("Status.Network.TotalMemorySegments", "0"));

						gen.writeNumberField("memorySegmentsAvailable", memorySegmentsAvailable);
						gen.writeNumberField("memorySegmentsTotal", memorySegmentsTotal);

						gen.writeArrayFieldStart("garbageCollectors");

						for (String gcName : metrics.garbageCollectorNames) {
							String count = metrics.getMetric("Status.JVM.GarbageCollector." + gcName + ".Count", null);
							String time = metrics.getMetric("Status.JVM.GarbageCollector." + gcName + ".Time", null);
							if (count != null && time != null) {
								gen.writeStartObject();
								gen.writeStringField("name", gcName);
								gen.writeNumberField("count", Long.valueOf(count));
								gen.writeNumberField("time", Long.valueOf(time));
								gen.writeEndObject();
							}
						}

						gen.writeEndArray();
						gen.writeEndObject();
					}
				}
			}

			gen.writeEndObject();
		}

		gen.writeEndArray();
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
