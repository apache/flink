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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.runtime.metrics.util.SystemResourcesMetricsInitializer.instantiateSystemMetrics;

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";
	private static final String METRICS_ACTOR_SYSTEM_NAME = "flink-metrics";

	static final String METRIC_GROUP_HEAP_NAME = "Heap";
	static final String METRIC_GROUP_NONHEAP_NAME = "NonHeap";

	private MetricUtils() {
	}

	public static ProcessMetricGroup instantiateProcessMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname,
			final Optional<Time> systemResourceProbeInterval) {
		final ProcessMetricGroup processMetricGroup = ProcessMetricGroup.create(metricRegistry, hostname);

		createAndInitializeStatusMetricGroup(processMetricGroup);

		systemResourceProbeInterval.ifPresent(interval -> instantiateSystemMetrics(processMetricGroup, interval));

		return processMetricGroup;
	}

	public static JobManagerMetricGroup instantiateJobManagerMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname) {
		final JobManagerMetricGroup jobManagerMetricGroup = new JobManagerMetricGroup(
			metricRegistry,
			hostname);

		return jobManagerMetricGroup;
	}

	public static Tuple2<TaskManagerMetricGroup, MetricGroup> instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			String hostName,
			ResourceID resourceID,
			Optional<Time> systemResourceProbeInterval) {
		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			hostName,
			resourceID.toString());

		MetricGroup statusGroup = createAndInitializeStatusMetricGroup(taskManagerMetricGroup);

		if (systemResourceProbeInterval.isPresent()) {
			instantiateSystemMetrics(taskManagerMetricGroup, systemResourceProbeInterval.get());
		}
		return Tuple2.of(taskManagerMetricGroup, statusGroup);
	}

	private static MetricGroup createAndInitializeStatusMetricGroup(AbstractMetricGroup<?> parentMetricGroup) {
		MetricGroup statusGroup = parentMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		instantiateStatusMetrics(statusGroup);
		return statusGroup;
	}

	public static void instantiateStatusMetrics(
			MetricGroup metricGroup) {
		MetricGroup jvm = metricGroup.addGroup("JVM");

		instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"));
		instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
		instantiateMemoryMetrics(jvm.addGroup("Memory"));
		instantiateThreadMetrics(jvm.addGroup("Threads"));
		instantiateCPUMetrics(jvm.addGroup("CPU"));
	}

	public static RpcService startRemoteMetricsRpcService(Configuration configuration, String hostname) throws Exception {
		final String portRange = configuration.getString(MetricOptions.QUERY_SERVICE_PORT);

		return startMetricRpcService(configuration, AkkaRpcServiceUtils.remoteServiceBuilder(configuration, hostname, portRange));
	}

	public static RpcService startLocalMetricsRpcService(Configuration configuration) throws Exception {
		return startMetricRpcService(configuration, AkkaRpcServiceUtils.localServiceBuilder(configuration));
	}

	private static RpcService startMetricRpcService(
			Configuration configuration, AkkaRpcServiceUtils.AkkaRpcServiceBuilder rpcServiceBuilder) throws Exception {
		final int threadPriority = configuration.getInteger(MetricOptions.QUERY_SERVICE_THREAD_PRIORITY);

		return rpcServiceBuilder
			.withActorSystemName(METRICS_ACTOR_SYSTEM_NAME)
			.withActorSystemExecutorConfiguration(new BootstrapTools.FixedThreadPoolExecutorConfiguration(1, 1, threadPriority))
			.createAndStart();
	}

	private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
		final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();
		metrics.<Long, Gauge<Long>>gauge("ClassesLoaded", mxBean::getTotalLoadedClassCount);
		metrics.<Long, Gauge<Long>>gauge("ClassesUnloaded", mxBean::getUnloadedClassCount);
	}

	private static void instantiateGarbageCollectorMetrics(MetricGroup metrics) {
		List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

		for (final GarbageCollectorMXBean garbageCollector: garbageCollectors) {
			MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());

			gcGroup.<Long, Gauge<Long>>gauge("Count", garbageCollector::getCollectionCount);
			gcGroup.<Long, Gauge<Long>>gauge("Time", garbageCollector::getCollectionTime);
		}
	}

	private static void instantiateMemoryMetrics(MetricGroup metrics) {
		instantiateHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_HEAP_NAME));
		instantiateNonHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_NONHEAP_NAME));

		final MBeanServer con = ManagementFactory.getPlatformMBeanServer();

		final String directBufferPoolName = "java.nio:type=BufferPool,name=direct";

		try {
			final ObjectName directObjectName = new ObjectName(directBufferPoolName);

			MetricGroup direct = metrics.addGroup("Direct");

			direct.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, directObjectName, "Count", -1L));
			direct.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, directObjectName, "MemoryUsed", -1L));
			direct.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, directObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", directBufferPoolName, e);
		}

		final String mappedBufferPoolName = "java.nio:type=BufferPool,name=mapped";

		try {
			final ObjectName mappedObjectName = new ObjectName(mappedBufferPoolName);

			MetricGroup mapped = metrics.addGroup("Mapped");

			mapped.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, mappedObjectName, "Count", -1L));
			mapped.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, mappedObjectName, "MemoryUsed", -1L));
			mapped.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, mappedObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", mappedBufferPoolName, e);
		}
	}

	@VisibleForTesting
	static void instantiateHeapMemoryMetrics(final MetricGroup metricGroup) {
		instantiateMemoryUsageMetrics(metricGroup, () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
	}

	@VisibleForTesting
	static void instantiateNonHeapMemoryMetrics(final MetricGroup metricGroup) {
		instantiateMemoryUsageMetrics(metricGroup, () -> ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
	}

	private static void instantiateMemoryUsageMetrics(final MetricGroup metricGroup, final Supplier<MemoryUsage> memoryUsageSupplier) {
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_USED, () -> memoryUsageSupplier.get().getUsed());
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_COMMITTED, () -> memoryUsageSupplier.get().getCommitted());
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_MAX, () -> memoryUsageSupplier.get().getMax());
	}

	private static void instantiateThreadMetrics(MetricGroup metrics) {
		final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

		metrics.<Integer, Gauge<Integer>>gauge("Count", mxBean::getThreadCount);
	}

	private static void instantiateCPUMetrics(MetricGroup metrics) {
		try {
			final com.sun.management.OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

			metrics.<Double, Gauge<Double>>gauge("Load", mxBean::getProcessCpuLoad);
			metrics.<Long, Gauge<Long>>gauge("Time", mxBean::getProcessCpuTime);
		} catch (Exception e) {
			LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
				" - CPU load metrics will not be available.", e);
		}
	}

	private static final class AttributeGauge<T> implements Gauge<T> {
		private final MBeanServer server;
		private final ObjectName objectName;
		private final String attributeName;
		private final T errorValue;

		private AttributeGauge(MBeanServer server, ObjectName objectName, String attributeName, T errorValue) {
			this.server = Preconditions.checkNotNull(server);
			this.objectName = Preconditions.checkNotNull(objectName);
			this.attributeName = Preconditions.checkNotNull(attributeName);
			this.errorValue = errorValue;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T getValue() {
			try {
				return (T) server.getAttribute(objectName, attributeName);
			} catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException e) {
				LOG.warn("Could not read attribute {}.", attributeName, e);
				return errorValue;
			}
		}
	}
}
