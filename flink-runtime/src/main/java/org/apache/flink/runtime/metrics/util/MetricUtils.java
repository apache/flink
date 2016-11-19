/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http//www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.metrics.util;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";
	private static final String VM_SIZE = "VmSize";
	private static final String VM_RSS = "VmRSS";

	private MetricUtils() {
	}

	public static void instantiateNetworkMetrics(
		MetricGroup metrics,
		final NetworkEnvironment network) {
		MetricGroup status = metrics.addGroup(METRIC_GROUP_STATUS_NAME);

		MetricGroup networkGroup = status
			.addGroup("Network");

		networkGroup.gauge("TotalMemorySegments", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return network.getNetworkBufferPool().getTotalNumberOfMemorySegments();
			}
		});
		networkGroup.gauge("AvailableMemorySegments", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return network.getNetworkBufferPool().getNumberOfAvailableMemorySegments();
			}
		});
	}

	public static void instantiateStatusMetrics(
		MetricGroup metrics) {
		MetricGroup status = metrics
			.addGroup(METRIC_GROUP_STATUS_NAME);

		MetricGroup jvm = status
			.addGroup("JVM");

		instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"));
		instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
		instantiateMemoryMetrics(jvm.addGroup("Memory"));
		instantiateThreadMetrics(jvm.addGroup("Threads"));
		instantiateCPUMetrics(jvm.addGroup("CPU"));

		MetricGroup process = status
			.addGroup("Process");

		instantiateProcessMemoryMetrics(process.addGroup("Memory"));
		instantiateProcessCPUMetrics(process.addGroup("CPU"));
	}

	private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
		final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

		metrics.gauge("ClassesLoaded", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getTotalLoadedClassCount();
			}
		});
		metrics.gauge("ClassesUnloaded", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getUnloadedClassCount();
			}
		});
	}

	private static void instantiateGarbageCollectorMetrics(MetricGroup metrics) {
		List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

		for (final GarbageCollectorMXBean garbageCollector : garbageCollectors) {
			MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());
			gcGroup.gauge("Count", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return garbageCollector.getCollectionCount();
				}
			});
			gcGroup.gauge("Time", new Gauge<Long>() {
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
		heap.gauge("Used", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getUsed();
			}
		});
		heap.gauge("Committed", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getCommitted();
			}
		});
		heap.gauge("Max", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getHeapMemoryUsage().getMax();
			}
		});

		MetricGroup nonHeap = metrics.addGroup("NonHeap");
		nonHeap.gauge("Used", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getUsed();
			}
		});
		nonHeap.gauge("Committed", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getCommitted();
			}
		});
		nonHeap.gauge("Max", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return mxBean.getNonHeapMemoryUsage().getMax();
			}
		});

		List<BufferPoolMXBean> bufferMxBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

		for (final BufferPoolMXBean bufferMxBean : bufferMxBeans) {
			MetricGroup bufferGroup = metrics.addGroup(WordUtils.capitalize(bufferMxBean.getName()));
			bufferGroup.gauge("Count", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return bufferMxBean.getCount();
				}
			});
			bufferGroup.gauge("MemoryUsed", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return bufferMxBean.getMemoryUsed();
				}
			});
			bufferGroup.gauge("TotalCapacity", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return bufferMxBean.getTotalCapacity();
				}
			});
		}
	}

	private static void instantiateThreadMetrics(MetricGroup metrics) {
		final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

		metrics.gauge("Count", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return mxBean.getThreadCount();
			}
		});
	}

	private static void instantiateCPUMetrics(MetricGroup metrics) {
		try {
			final OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();

			final Method fetchCPULoadMethod = Class.forName("com.sun.management.OperatingSystemMXBean")
				.getMethod("getProcessCpuLoad");
			// verify that we can invoke the method
			fetchCPULoadMethod.invoke(mxBean);

			final Method fetchCPUTimeMethod = Class.forName("com.sun.management.OperatingSystemMXBean")
				.getMethod("getProcessCpuTime");
			// verify that we can invoke the method
			fetchCPUTimeMethod.invoke(mxBean);

			metrics.gauge("Load", new Gauge<Double>() {
				@Override
				public Double getValue() {
					try {
						return (Double) fetchCPULoadMethod.invoke(mxBean);
					} catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException ignored) {
						return -1.0;
					}
				}
			});
			metrics.gauge("Time", new Gauge<Long>() {
				@Override
				public Long getValue() {
					try {
						return (Long) fetchCPUTimeMethod.invoke(mxBean);
					} catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException ignored) {
						return -1L;
					}
				}
			});
		} catch (ClassNotFoundException | InvocationTargetException | SecurityException | NoSuchMethodException | IllegalArgumentException | IllegalAccessException ignored) {
			LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
				" - CPU load metrics will not be available.");
			// make sure that a metric still exists for the given name
			metrics.gauge("Load", new Gauge<Double>() {
				@Override
				public Double getValue() {
					return -1.0;
				}
			});
			metrics.gauge("Time", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return -1L;
				}
			});
		}
	}

	private static void instantiateProcessMemoryMetrics(MetricGroup metrics) {
		String os = System.getProperty("os.name");
		if (os.toLowerCase().startsWith("linux")) {
			metrics.gauge(VM_SIZE, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return getProcessVmStat(VM_SIZE);
				}
			});
			metrics.gauge(VM_RSS, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return getProcessVmStat(VM_RSS);
				}
			});
		} else {
			LOG.warn("Unsupported process memory metrics in current os " + os);
			metrics.gauge(VM_SIZE, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return -1L;
				}
			});
			metrics.gauge(VM_RSS, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return -1L;
				}
			});
		}
	}

	private static void instantiateProcessCPUMetrics(MetricGroup metrics) {
		String os = System.getProperty("os.name");
		if (os.toLowerCase().startsWith("linux")) {
			metrics.gauge("Usage", new Gauge<Double>() {
				@Override
				public Double getValue() {
					return ProcessCpuStat.getCpuUsage();
				}
			});
		} else {
			LOG.warn("Unsupported process CPU metrics in current os " + os);
			metrics.gauge("Usage", new Gauge<Double>() {
				@Override
				public Double getValue() {
					return 0.0;
				}
			});
		}
	}

	private static long getProcessVmStat(String vmProperty) {
		BufferedReader br = null;
		String line;

		try {
			br = new BufferedReader(new FileReader("/proc/self/status"));
			//read process status
			while ((line = br.readLine()) != null) {
				if (line.startsWith(vmProperty)) {
					int beginIndex = line.indexOf(":") + 1;
					int endIndex = line.indexOf("kB") - 1;
					String value = line.substring(beginIndex, endIndex).trim();
					return Long.parseLong(value);
				}
			}
		} catch (IOException ignore) {
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException ignore) {
			}
		}

		return -1L;
	}

	private static class ProcessCpuStat {

		private static final int AVAILABLE_PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

		private static double currentSystemCpuTotal = 0.0;
		private static double currentProcCpuClock = 0.0;

		public static double getCpuUsage() {
			try {
				double procCpuClock = getProcessCpuClock();
				double totalCpuStat = getTotalCpuClock();
				if (totalCpuStat == 0.0) {
					return 0.0;
				}

				double cpuUsagePercent = procCpuClock / totalCpuStat;
				return cpuUsagePercent * AVAILABLE_PROCESSOR_COUNT;
			} catch (IOException ex) {
				LOG.warn("collect cpu load exception " + ex.getMessage());
			}

			return 0.0;
		}

		private static double getProcessCpuClock() throws IOException {
			double lastProcCpuClock = currentProcCpuClock;

			String line = getFirstLineFromFile("/proc/self/stat");
			if (line == null) {
				throw new IOException("read /proc/self/stat null !");
			}

			String[] processStats = line.split(" ", -1);
			if (processStats.length < 17) {
				LOG.error("parse cpu stat file failed! the first line is:" + line);
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

			double cpuTotal = 0.0;
			for (int i = rBracketPos + 12; i < rBracketPos + 16; i++) {
				cpuTotal += Double.parseDouble(processStats[i]);
			}

			currentProcCpuClock = cpuTotal;

			return currentProcCpuClock - lastProcCpuClock;
		}

		private static double getTotalCpuClock() throws IOException {
			double lastSystemCpuTotal = currentSystemCpuTotal;

			String line = getFirstLineFromFile("/proc/stat");
			if (line == null) {
				throw new IOException("read /proc/stat null !");
			}

			String[] cpuStats = line.split(" ", -1);
			if (cpuStats.length < 11) {
				LOG.error("parse cpu stat file failed! the first line is:" + line);
				throw new IOException("parse cpu stat file failed!");
			}

			double statCpuTotal = 0.0;
			for (int i = 2; i < cpuStats.length; i++) {
				statCpuTotal += Double.parseDouble(cpuStats[i]);
			}

			currentSystemCpuTotal = statCpuTotal;

			return currentSystemCpuTotal - lastSystemCpuTotal;
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
	}
}
