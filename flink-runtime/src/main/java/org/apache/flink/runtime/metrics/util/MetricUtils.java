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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
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
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";

	private MetricUtils() {
	}

	public static JobManagerMetricGroup instantiateJobManagerMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname) {
		final JobManagerMetricGroup jobManagerMetricGroup = new JobManagerMetricGroup(
			metricRegistry,
			hostname);

		MetricGroup statusGroup = jobManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// initialize the JM metrics
		instantiateStatusMetrics(statusGroup);

		return jobManagerMetricGroup;
	}

	public static TaskManagerMetricGroup instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			TaskManagerLocation taskManagerLocation,
			NetworkEnvironment network) {
		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			taskManagerLocation.getHostname(),
			taskManagerLocation.getResourceID().toString());

		MetricGroup statusGroup = taskManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// Initialize the TM metrics
		instantiateStatusMetrics(statusGroup);

		MetricGroup networkGroup = statusGroup
			.addGroup("Network");
		instantiateNetworkMetrics(networkGroup, network);

		return taskManagerMetricGroup;
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

	private static void instantiateNetworkMetrics(
		MetricGroup metrics,
		final NetworkEnvironment network) {
		metrics.<Long, Gauge<Long>>gauge("TotalMemorySegments", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return (long) network.getNetworkBufferPool().getTotalNumberOfMemorySegments();
			}
		});

		metrics.<Long, Gauge<Long>>gauge("AvailableMemorySegments", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return (long) network.getNetworkBufferPool().getNumberOfAvailableMemorySegments();
			}
		});
	}

	private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
		final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

		metrics.<Long, Gauge<Long>>gauge("ClassesLoaded", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getTotalLoadedClassCount();
			}
		});

		metrics.<Long, Gauge<Long>>gauge("ClassesUnloaded", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getUnloadedClassCount();
			}
		});
	}

	private static void instantiateGarbageCollectorMetrics(MetricGroup metrics) {
		List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

		for (final GarbageCollectorMXBean garbageCollector: garbageCollectors) {
			MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());

			gcGroup.<Long, Gauge<Long>>gauge("Count", new Gauge<Long> () {
				@Override
				public Long getValue() {
					return garbageCollector.getCollectionCount();
				}
			});

			gcGroup.<Long, Gauge<Long>>gauge("Time", new Gauge<Long> () {
				@Override
				public Long getValue() {
					return garbageCollector.getCollectionTime();
				}
			});
		}
	}

	private static void instantiateMemoryMetrics(MetricGroup metrics) {
		final MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();

		MetricGroup heap = metrics.addGroup("Heap");

		heap.<Long, Gauge<Long>>gauge("Used", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getUsed();
			}
		});
		heap.<Long, Gauge<Long>>gauge("Committed", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getCommitted();
			}
		});
		heap.<Long, Gauge<Long>>gauge("Max", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getMax();
			}
		});

		MetricGroup nonHeap = metrics.addGroup("NonHeap");

		nonHeap.<Long, Gauge<Long>>gauge("Used", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getUsed();
			}
		});
		nonHeap.<Long, Gauge<Long>>gauge("Committed", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getCommitted();
			}
		});
		nonHeap.<Long, Gauge<Long>>gauge("Max", new Gauge<Long> () {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getMax();
			}
		});

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

	private static void instantiateThreadMetrics(MetricGroup metrics) {
		final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

		metrics.<Integer, Gauge<Integer>>gauge("Count", new Gauge<Integer> () {
			@Override
			public Integer getValue() {
				return mxBean.getThreadCount();
			}
		});
	}

	private static void instantiateCPUMetrics(MetricGroup metrics) {
		try {
			final com.sun.management.OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

			metrics.<Double, Gauge<Double>>gauge("Load", new Gauge<Double> () {
				@Override
				public Double getValue() {
					return mxBean.getProcessCpuLoad();
				}
			});
			metrics.<Long, Gauge<Long>>gauge("Time", new Gauge<Long> () {
				@Override
				public Long getValue() {
					return mxBean.getProcessCpuTime();
				}
			});
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
