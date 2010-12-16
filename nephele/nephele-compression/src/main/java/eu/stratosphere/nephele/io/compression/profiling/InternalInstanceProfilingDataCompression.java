/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.compression.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;

public class InternalInstanceProfilingDataCompression implements IOReadableWritable {

	private int blocksFromDevice;

	private int blocksToDevice;

	private long freeMemory;

	private int ioWaitCPU;

	private int idleCPU;

	private long inactiveMemory;

	private InstanceConnectionInfo instanceConnectionInfo;

	private int profilingInterval;

	private long swapMemory;

	private long swappedFromDisk;

	private long swappedToDisk;

	private int systemCPU;

	private long totalMemory;

	private long usedMemory;

	private int userCPU;

	private long receivedBytes;

	private long transmittedBytes;

	private long goodput;

	private long compressionTime;

	private double compressionRatio;

	public InternalInstanceProfilingDataCompression() {
		this.blocksFromDevice = -1;
		this.blocksToDevice = -1;
		this.freeMemory = -1;
		this.ioWaitCPU = -1;
		this.idleCPU = -1;
		this.inactiveMemory = -1;
		this.instanceConnectionInfo = new InstanceConnectionInfo();
		this.profilingInterval = -1;
		this.swapMemory = -1;
		this.swappedFromDisk = -1;
		this.swappedToDisk = -1;
		this.systemCPU = -1;
		this.totalMemory = -1;
		this.usedMemory = -1;
		this.userCPU = -1;
		this.receivedBytes = -1;
		this.transmittedBytes = -1;

		this.compressionRatio = 1;
		this.compressionTime = -1;
	}

	public InternalInstanceProfilingDataCompression(InstanceConnectionInfo instanceConnectionInfo, int profilingInterval) {

		this.instanceConnectionInfo = instanceConnectionInfo;
		this.profilingInterval = profilingInterval;
		this.blocksFromDevice = -1;
		this.blocksToDevice = -1;
		this.freeMemory = -1;
		this.ioWaitCPU = -1;
		this.idleCPU = -1;
		this.inactiveMemory = -1;
		this.swapMemory = -1;
		this.swappedFromDisk = -1;
		this.swappedToDisk = -1;
		this.systemCPU = -1;
		this.totalMemory = -1;
		this.usedMemory = -1;
		this.userCPU = -1;
		this.receivedBytes = -1;
		this.transmittedBytes = -1;
	}

	public int getBlocksFromDevice() {
		return this.blocksFromDevice;
	}

	public int getBlocksToDevice() {
		return this.blocksToDevice;
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

	public long getInactiveMemory() {
		return this.inactiveMemory;
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	public int getProfilingInterval() {
		return this.profilingInterval;
	}

	public long getSwapMemory() {
		return this.swapMemory;
	}

	public long getSwappedFromDisk() {
		return this.swappedFromDisk;
	}

	public long getSwappedToDisk() {
		return this.swappedToDisk;
	}

	public int getSystemCPU() {
		return this.systemCPU;
	}

	public long getTotalMemory() {
		return this.totalMemory;
	}

	public long getUsedMemory() {
		return this.usedMemory;
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

	@Override
	public void read(DataInput in) throws IOException {

		this.blocksFromDevice = in.readInt();
		this.blocksToDevice = in.readInt();
		this.freeMemory = in.readLong();
		this.ioWaitCPU = in.readInt();
		this.idleCPU = in.readInt();
		this.inactiveMemory = in.readLong();
		this.instanceConnectionInfo.read(in);
		this.profilingInterval = in.readInt();
		this.swapMemory = in.readLong();
		this.swappedFromDisk = in.readLong();
		this.swappedToDisk = in.readLong();
		this.systemCPU = in.readInt();
		this.totalMemory = in.readLong();
		this.usedMemory = in.readLong();
		this.userCPU = in.readInt();
		this.receivedBytes = in.readLong();
		this.transmittedBytes = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(this.blocksFromDevice);
		out.writeInt(this.blocksToDevice);
		out.writeLong(this.freeMemory);
		out.writeInt(this.ioWaitCPU);
		out.writeInt(this.idleCPU);
		out.writeLong(this.inactiveMemory);
		this.instanceConnectionInfo.write(out);
		out.writeInt(this.profilingInterval);
		out.writeLong(this.swapMemory);
		out.writeLong(this.swappedFromDisk);
		out.writeLong(this.swappedToDisk);
		out.writeInt(this.systemCPU);
		out.writeLong(this.totalMemory);
		out.writeLong(this.usedMemory);
		out.writeInt(this.userCPU);
		out.writeLong(this.receivedBytes);
		out.writeLong(this.transmittedBytes);

	}

	public void setBlocksFromDevice(int blocksFromDevice) {
		this.blocksFromDevice = blocksFromDevice;
	}

	public void setBlocksToDevice(int blocksToDevice) {
		this.blocksToDevice = blocksToDevice;
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

	public void setInactiveMemory(long inactiveMemory) {
		this.inactiveMemory = inactiveMemory;
	}

	public void setSwapMemory(long swapMemory) {
		this.swapMemory = swapMemory;
	}

	public void setSwappedFromDisk(long swappedFromDisk) {
		this.swappedFromDisk = swappedFromDisk;
	}

	public void setSwappedToDisk(long swappedToDisk) {
		this.swappedToDisk = swappedToDisk;
	}

	public void setSystemCPU(int systemCPU) {
		this.systemCPU = systemCPU;
	}

	public void setTotalMemory(long totalMemory) {
		this.totalMemory = totalMemory;
	}

	public void setUsedMemory(long usedMemory) {
		this.usedMemory = usedMemory;
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

	public long getCompressionTime() {
		return compressionTime;
	}

	public void setCompressionTime(long compressionTime) {
		this.compressionTime = compressionTime;
	}

	public double getCompressionRatio() {
		return compressionRatio;
	}

	public void setCompressionRatio(double compressionRatio) {
		this.compressionRatio = compressionRatio;
	}

	public long getGoodput() {
		return goodput;
	}

	public void setGoodput(long goodput) {
		this.goodput = goodput;
	}

}
