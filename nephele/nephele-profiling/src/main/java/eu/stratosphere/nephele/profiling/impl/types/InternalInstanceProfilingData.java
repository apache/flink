/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl.types;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

public class InternalInstanceProfilingData implements InternalProfilingData {

	private final InstanceConnectionInfo instanceConnectionInfo;

	private final int profilingInterval;

	private int ioWaitCPU;

	private int idleCPU;

	private int userCPU;

	private int systemCPU;

	private int hardIrqCPU;

	private int softIrqCPU;

	private long totalMemory;

	private long freeMemory;

	private long bufferedMemory;

	private long cachedMemory;

	private long cachedSwapMemory;

	private long receivedBytes;

	private long transmittedBytes;

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InternalInstanceProfilingData() {
		this.freeMemory = -1L;
		this.ioWaitCPU = -1;
		this.idleCPU = -1;
		this.instanceConnectionInfo = null;
		this.profilingInterval = -1;
		this.systemCPU = -1;
		this.hardIrqCPU = -1;
		this.softIrqCPU = -1;
		this.totalMemory = -1L;
		this.bufferedMemory = -1L;
		this.cachedMemory = -1L;
		this.cachedSwapMemory = -1L;
		this.userCPU = -1;
		this.receivedBytes = -1L;
		this.transmittedBytes = -1L;
	}

	public InternalInstanceProfilingData(final InstanceConnectionInfo instanceConnectionInfo,
			final int profilingInterval) {

		this.instanceConnectionInfo = instanceConnectionInfo;
		this.profilingInterval = profilingInterval;
		this.freeMemory = -1L;
		this.ioWaitCPU = -1;
		this.idleCPU = -1;
		this.systemCPU = -1;
		this.hardIrqCPU = -1;
		this.softIrqCPU = -1;
		this.totalMemory = -1L;
		this.bufferedMemory = -1L;
		this.cachedMemory = -1L;
		this.cachedSwapMemory = -1L;
		this.userCPU = -1;
		this.receivedBytes = -1L;
		this.transmittedBytes = -1L;
	}

	public long getFreeMemory() {
		return this.freeMemory;
	}

	public int getIOWaitCPU() {
		return this.ioWaitCPU;
	}

	public int getIdleCPU() {
		return this.idleCPU;
	}

	public int getHardIrqCPU() {
		return this.hardIrqCPU;
	}

	public int getSoftIrqCPU() {
		return this.softIrqCPU;
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	public int getProfilingInterval() {
		return this.profilingInterval;
	}

	public int getSystemCPU() {
		return this.systemCPU;
	}

	public long getTotalMemory() {
		return this.totalMemory;
	}

	public long getBufferedMemory() {
		return this.bufferedMemory;
	}

	public long getCachedMemory() {
		return this.cachedMemory;
	}

	public long getCachedSwapMemory() {
		return this.cachedSwapMemory;
	}

	public int getUserCPU() {
		return this.userCPU;
	}

	public long getReceivedBytes() {
		return this.receivedBytes;
	}

	public long getTransmittedBytes() {
		return this.transmittedBytes;
	}

	public void setFreeMemory(long freeMemory) {
		this.freeMemory = freeMemory;
	}

	public void setIoWaitCPU(int ioWaitCPU) {
		this.ioWaitCPU = ioWaitCPU;
	}

	public void setIdleCPU(int idleCPU) {
		this.idleCPU = idleCPU;
	}

	public void setSystemCPU(int systemCPU) {
		this.systemCPU = systemCPU;
	}

	public void setHardIrqCPU(int hardIrqCPU) {
		this.hardIrqCPU = hardIrqCPU;
	}

	public void setSoftIrqCPU(int softIrqCPU) {
		this.softIrqCPU = softIrqCPU;
	}

	public void setTotalMemory(long totalMemory) {
		this.totalMemory = totalMemory;
	}

	public void setBufferedMemory(long bufferedMemory) {
		this.bufferedMemory = bufferedMemory;
	}

	public void setCachedMemory(long cachedMemory) {
		this.cachedMemory = cachedMemory;
	}

	public void setCachedSwapMemory(long cachedSwapMemory) {
		this.cachedSwapMemory = cachedSwapMemory;
	}

	public void setUserCPU(int userCPU) {
		this.userCPU = userCPU;
	}

	public void setReceivedBytes(long receivedBytes) {
		this.receivedBytes = receivedBytes;
	}

	public void setTransmittedBytes(long transmittedBytes) {
		this.transmittedBytes = transmittedBytes;
	}

}
