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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * Utility class to initialize system resource metrics.
 */
public class SystemResourcesMetricsInitializer {
	private static final Logger LOG = LoggerFactory.getLogger(SystemResourcesMetricsInitializer.class);

	public static void instantiateSystemMetrics(MetricGroup metricGroup, Time probeInterval) {
		try {
			MetricGroup system = metricGroup.addGroup("System");

			SystemResourcesCounter systemResourcesCounter = new SystemResourcesCounter(probeInterval);
			systemResourcesCounter.start();

			SystemInfo systemInfo = new SystemInfo();
			HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();

			instantiateMemoryMetrics(system.addGroup("Memory"), hardwareAbstractionLayer.getMemory());
			instantiateSwapMetrics(system.addGroup("Swap"), hardwareAbstractionLayer.getMemory());
			instantiateCPUMetrics(system.addGroup("CPU"), systemResourcesCounter);
			instantiateNetworkMetrics(system.addGroup("Network"), systemResourcesCounter);
		}
		catch (NoClassDefFoundError ex) {
			LOG.warn(
				"Failed to initialize system resource metrics because of missing class definitions." +
				" Did you forget to explicitly add the oshi-core optional dependency?",
				ex);
		}
	}

	private static void instantiateMemoryMetrics(MetricGroup metrics, GlobalMemory memory) {
		metrics.<Long, Gauge<Long>>gauge("Available", memory::getAvailable);
		metrics.<Long, Gauge<Long>>gauge("Total", memory::getTotal);
	}

	private static void instantiateSwapMetrics(MetricGroup metrics, GlobalMemory memory) {
		metrics.<Long, Gauge<Long>>gauge("Used", memory::getSwapUsed);
		metrics.<Long, Gauge<Long>>gauge("Total", memory::getSwapTotal);
	}

	private static void instantiateCPUMetrics(MetricGroup metrics, SystemResourcesCounter usageCounter) {
		metrics.<Double, Gauge<Double>>gauge("Usage", usageCounter::getCpuUsage);
		metrics.<Double, Gauge<Double>>gauge("Idle", usageCounter::getCpuIdle);
		metrics.<Double, Gauge<Double>>gauge("Sys", usageCounter::getCpuSys);
		metrics.<Double, Gauge<Double>>gauge("User", usageCounter::getCpuUser);
		metrics.<Double, Gauge<Double>>gauge("IOWait", usageCounter::getIOWait);
		metrics.<Double, Gauge<Double>>gauge("Nice", usageCounter::getCpuNice);
		metrics.<Double, Gauge<Double>>gauge("Irq", usageCounter::getCpuIrq);
		metrics.<Double, Gauge<Double>>gauge("SoftIrq", usageCounter::getCpuSoftIrq);

		metrics.<Double, Gauge<Double>>gauge("Load1min", usageCounter::getCpuLoad1);
		metrics.<Double, Gauge<Double>>gauge("Load5min", usageCounter::getCpuLoad5);
		metrics.<Double, Gauge<Double>>gauge("Load15min", usageCounter::getCpuLoad15);

		for (int i = 0; i < usageCounter.getProcessorsCount(); i++) {
			final int processor = i;
			metrics.<Double, Gauge<Double>>gauge(
				String.format("UsageCPU%d", processor),
				() -> usageCounter.getCpuUsagePerProcessor(processor));
		}
	}

	private static void instantiateNetworkMetrics(MetricGroup metrics, SystemResourcesCounter usageCounter) {
		for (int i = 0; i < usageCounter.getNetworkInterfaceNames().length; i++) {
			MetricGroup interfaceGroup = metrics.addGroup(usageCounter.getNetworkInterfaceNames()[i]);

			final int interfaceNo = i;
			interfaceGroup.<Long, Gauge<Long>>gauge("ReceiveRate", () -> usageCounter.getReceiveRatePerInterface(interfaceNo));
			interfaceGroup.<Long, Gauge<Long>>gauge("SendRate", () -> usageCounter.getSendRatePerInterface(interfaceNo));
		}
	}
}
