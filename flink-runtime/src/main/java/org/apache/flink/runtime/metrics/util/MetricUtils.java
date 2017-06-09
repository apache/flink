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

import org.apache.commons.lang3.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";

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
}
