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
import org.apache.flink.util.OperatingSystem;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
	private static final boolean IS_PROC_FS_AVAILABLE = OperatingSystem.isLinux();

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

		if (IS_PROC_FS_AVAILABLE) {
			MetricGroup process = metrics.addGroup("Process");
			process.gauge("RSS", ProcessMemCollector::getRssMem);
			process.gauge("Total", ProcessMemCollector::getTotalMem);
		}

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
			if (IS_PROC_FS_AVAILABLE) {
				metrics.gauge("Usage", new Gauge<Double>() {
					@Override
					public Double getValue() {
						return ProcessCpuCollector.getUsage();
					}
				});
			}
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

	/**
	 * Collect process CPU metrics.
	 */
	public static class ProcessCpuCollector {
		private static final Logger LOG = LoggerFactory.getLogger(ProcessCpuCollector.class);

		private static final String CPU_STAT_FILE = "/proc/stat";
		private static final String PROCESS_STAT_FILE = "/proc/self/stat";
		private static final int AVAILABLE_PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

		private static double currentSystemCpuTotal = 0;
		private static double currentProcCpuClock = 0;

		public static double getUsage() {
			try {
				double procCpuClock = getProcessCpuClock();
				double totalCpuStat = getTotalCpuClock();
				if (totalCpuStat == 0) {
					return 0;
				}
				return procCpuClock / totalCpuStat * AVAILABLE_PROCESSOR_COUNT;
			} catch (IOException ex) {
				LOG.warn("collect cpu load exception " + ex.getMessage());
				return 0;
			}
		}

		private static String getFirstLineFromFile(String fileName) throws IOException {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(fileName));
				String line = br.readLine();
				return line;
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}

		private static double getProcessCpuClock() throws IOException {
			double lastProcCpuClock = currentProcCpuClock;
			String content = getFirstLineFromFile(PROCESS_STAT_FILE);
			if (content == null) {
				throw new IOException("read /proc/self/stat null !");
			}
			String[] processStats = content.split(" ", -1);
			if (processStats.length < 17) {
				LOG.error("parse cpu stat file failed! the first line is:" + content);
				throw new IOException("parse process stat file failed!");
			}
			int rBracketPos = 0;
			for (int i = processStats.length - 1; i > 0; i--) {
				if (processStats[i].contains(")")) {
					rBracketPos = i;
					break;
				}
			}
			if (rBracketPos == 0) {
				throw new IOException("get right bracket pos error");
			}
			double cpuTotal = 0;
			for (int i = rBracketPos + 12; i < rBracketPos + 16; i++) {
				cpuTotal += Double.parseDouble(processStats[i]);
			}
			currentProcCpuClock = cpuTotal;
			return currentProcCpuClock - lastProcCpuClock;
		}

		private static double getTotalCpuClock() throws IOException {
			double lastSystemCpuTotal = currentSystemCpuTotal;
			String line = getFirstLineFromFile(CPU_STAT_FILE);
			if (line == null) {
				throw new IOException("read /proc/stat null !");
			}
			String[] cpuStats = line.split(" ", -1);
			if (cpuStats.length < 11) {
				LOG.error("parse cpu stat file failed! the first line is:" + line);
				throw new IOException("parse cpu stat file failed!");
			}
			double statCpuTotal = 0;
			for (int i = 2; i < cpuStats.length; i++) {
				statCpuTotal += Double.parseDouble(cpuStats[i]);
			}
			currentSystemCpuTotal = statCpuTotal;

			return currentSystemCpuTotal - lastSystemCpuTotal;
		}
	}

	/**
	 * Collect process memory metrics.
	 */
	public static class ProcessMemCollector {
		private static final Logger LOG = LoggerFactory.getLogger(ProcessMemCollector.class);
		private static final double KILO = 1024;
		private static final String MEMORY_TOTAL_USE_TOKEN = "VmSize";
		private static final String MEMORY_RSS_USE_TOKEN = "VmRSS";
		private static final String MEM_STAT_FILE = "/proc/self/status";

		public static double getTotalMem() {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(MEM_STAT_FILE));
				while (true) {
					String line = br.readLine();
					if (null == line) {
						break;
					}
					if (line.startsWith(MEMORY_TOTAL_USE_TOKEN)) {
						return getNumber(line);
					}
				}
			} catch (IOException ex) {
				LOG.warn("collect mem use exception " + ex.getMessage());
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException ex) {
					}
				}
			}
			return 0.0;
		}

		public static long getRssMem() {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(MEM_STAT_FILE));
				while (true) {
					String line = br.readLine();
					if (null == line) {
						break;
					}
					if (line.startsWith(MEMORY_RSS_USE_TOKEN)) {
						return getNumber(line);
					}
				}
			} catch (IOException ex) {
				LOG.warn("collect mem use exception " + ex.getMessage());
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException ex) {
					}
				}
			}
			return 0L;
		}

		private static long getNumber(String line) {
			int beginIndex = line.indexOf(":") + 1;
			int endIndex = line.indexOf("kB") - 1;
			String memSize = line.substring(beginIndex, endIndex).trim();
			// Convert KB to Byte
			return 1024L * Long.parseLong(memSize);
		}
	}
}
