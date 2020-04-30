/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.api.common.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Daemon thread probing system resources.
 *
 * <p>To accurately and consistently report CPU and network usage we have to periodically probe
 * CPU ticks and network sent/received bytes and then convert those values to CPU usage and
 * send/receive byte rates.
 */
@ThreadSafe
public class SystemResourcesCounter extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(SystemResourcesCounter.class);

	private final long probeIntervalMs;
	private final SystemInfo systemInfo = new SystemInfo();
	private final HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();

	private volatile boolean running = true;

	private long[] previousCpuTicks;
	private long[] bytesReceivedPerInterface;
	private long[] bytesSentPerInterface;

	private volatile double cpuUser;
	private volatile double cpuNice;
	private volatile double cpuSys;
	private volatile double cpuIdle;
	private volatile double cpuIOWait;
	private volatile double cpuIrq;
	private volatile double cpuSoftIrq;
	private volatile double cpuUsage;

	private volatile double cpuLoad1;
	private volatile double cpuLoad5;
	private volatile double cpuLoad15;

	private AtomicReferenceArray<Double> cpuUsagePerProcessor;

	private final String[] networkInterfaceNames;

	private AtomicLongArray receiveRatePerInterface;
	private AtomicLongArray sendRatePerInterface;

	public SystemResourcesCounter(Time probeInterval) {
		probeIntervalMs = probeInterval.toMilliseconds();
		checkState(this.probeIntervalMs > 0);

		setName(SystemResourcesCounter.class.getSimpleName() + " probing thread");

		cpuUsagePerProcessor = new AtomicReferenceArray<>(hardwareAbstractionLayer.getProcessor().getLogicalProcessorCount());

		NetworkIF[] networkIFs = hardwareAbstractionLayer.getNetworkIFs();
		bytesReceivedPerInterface = new long[networkIFs.length];
		bytesSentPerInterface = new long[networkIFs.length];
		receiveRatePerInterface = new AtomicLongArray(networkIFs.length);
		sendRatePerInterface = new AtomicLongArray(networkIFs.length);
		networkInterfaceNames = new String[networkIFs.length];

		for (int i = 0; i < networkInterfaceNames.length; i++) {
			networkInterfaceNames[i] = networkIFs[i].getName();
		}
	}

	@Override
	public void run() {
		try {
			while (running) {
				calculateCPUUsage(hardwareAbstractionLayer.getProcessor());
				calculateNetworkUsage(hardwareAbstractionLayer.getNetworkIFs());
				Thread.sleep(probeIntervalMs);
			}
		} catch (InterruptedException e) {
			if (running) {
				LOG.warn("{} has failed", SystemResourcesCounter.class.getSimpleName(), e);
			}
		}
	}

	public void shutdown() throws InterruptedException {
		running = false;
		interrupt();
		join();
	}

	public double getCpuUser() {
		return cpuUser;
	}

	public double getCpuNice() {
		return cpuNice;
	}

	public double getCpuSys() {
		return cpuSys;
	}

	public double getCpuIdle() {
		return cpuIdle;
	}

	public double getIOWait() {
		return cpuIOWait;
	}

	public double getCpuIrq() {
		return cpuIrq;
	}

	public double getCpuSoftIrq() {
		return cpuSoftIrq;
	}

	public double getCpuUsage() {
		return cpuUsage;
	}

	public double getCpuLoad1() {
		return cpuLoad1;
	}

	public double getCpuLoad5() {
		return cpuLoad5;
	}

	public double getCpuLoad15() {
		return cpuLoad15;
	}

	public int getProcessorsCount() {
		return cpuUsagePerProcessor.length();
	}

	public double getCpuUsagePerProcessor(int processor) {
		return cpuUsagePerProcessor.get(processor);
	}

	public String[] getNetworkInterfaceNames() {
		return networkInterfaceNames;
	}

	public long getReceiveRatePerInterface(int interfaceNo) {
		return receiveRatePerInterface.get(interfaceNo);
	}

	public long getSendRatePerInterface(int interfaceNo) {
		return sendRatePerInterface.get(interfaceNo);
	}

	private void calculateCPUUsage(CentralProcessor processor) {
		long[] ticks = processor.getSystemCpuLoadTicks();
		if (this.previousCpuTicks == null) {
			this.previousCpuTicks = ticks;
		}

		long userTicks = ticks[TickType.USER.getIndex()] - previousCpuTicks[TickType.USER.getIndex()];
		long niceTicks = ticks[TickType.NICE.getIndex()] - previousCpuTicks[TickType.NICE.getIndex()];
		long sysTicks = ticks[TickType.SYSTEM.getIndex()] - previousCpuTicks[TickType.SYSTEM.getIndex()];
		long idleTicks = ticks[TickType.IDLE.getIndex()] - previousCpuTicks[TickType.IDLE.getIndex()];
		long iowaitTicks = ticks[TickType.IOWAIT.getIndex()] - previousCpuTicks[TickType.IOWAIT.getIndex()];
		long irqTicks = ticks[TickType.IRQ.getIndex()] - previousCpuTicks[TickType.IRQ.getIndex()];
		long softIrqTicks = ticks[TickType.SOFTIRQ.getIndex()] - previousCpuTicks[TickType.SOFTIRQ.getIndex()];
		long totalCpuTicks = userTicks + niceTicks + sysTicks + idleTicks + iowaitTicks + irqTicks + softIrqTicks;
		this.previousCpuTicks = ticks;

		cpuUser = 100d * userTicks / totalCpuTicks;
		cpuNice = 100d * niceTicks / totalCpuTicks;
		cpuSys = 100d * sysTicks / totalCpuTicks;
		cpuIdle = 100d * idleTicks / totalCpuTicks;
		cpuIOWait = 100d * iowaitTicks / totalCpuTicks;
		cpuIrq = 100d * irqTicks / totalCpuTicks;
		cpuSoftIrq = 100d * softIrqTicks / totalCpuTicks;

		cpuUsage = processor.getSystemCpuLoad() * 100;

		double[] loadAverage = processor.getSystemLoadAverage(3);
		cpuLoad1 = (loadAverage[0] < 0 ? Double.NaN : loadAverage[0]);
		cpuLoad5 = (loadAverage[1] < 0 ? Double.NaN : loadAverage[1]);
		cpuLoad15 = (loadAverage[2] < 0 ? Double.NaN : loadAverage[2]);

		double[] load = processor.getProcessorCpuLoadBetweenTicks();
		checkState(load.length == cpuUsagePerProcessor.length());
		for (int i = 0; i < load.length; i++) {
			cpuUsagePerProcessor.set(i, load[i] * 100);
		}
	}

	private void calculateNetworkUsage(NetworkIF[] networkIFs) {
		checkState(networkIFs.length == receiveRatePerInterface.length());

		for (int i = 0; i < networkIFs.length; i++) {
			NetworkIF networkIF = networkIFs[i];
			networkIF.updateNetworkStats();

			receiveRatePerInterface.set(i, (networkIF.getBytesRecv() - bytesReceivedPerInterface[i]) * 1000 / probeIntervalMs);
			sendRatePerInterface.set(i, (networkIF.getBytesSent() - bytesSentPerInterface[i]) * 1000 / probeIntervalMs);

			bytesReceivedPerInterface[i] = networkIF.getBytesRecv();
			bytesSentPerInterface[i] = networkIF.getBytesSent();
		}
	}
}
