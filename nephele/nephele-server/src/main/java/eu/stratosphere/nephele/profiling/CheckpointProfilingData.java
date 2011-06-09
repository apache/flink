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
package eu.stratosphere.nephele.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * @author marrus
 *
 */
public class CheckpointProfilingData implements  IOReadableWritable{

	

	private int ioWaitCPU;

	private int idleCPU;

	private int userCPU;

	private int systemCPU;

	private int hardIrqCPU;

	private int softIrqCPU;

	private long receivedBytes;

	private long transmittedBytes;

	private int allReadRecords;

	private int allWrittenRecords;

	/**
	 * Empty constructor used to deserialize the object.
	 *
	 */
	public CheckpointProfilingData() {
		this.ioWaitCPU = -1;
		this.idleCPU = -1;
		this.systemCPU = -1;
		this.userCPU = -1;
		this.receivedBytes = -1;
		this.transmittedBytes = -1;
		this.allReadRecords = -1;
		this.allWrittenRecords = -1;
	}


	public CheckpointProfilingData(int ioWaitCPU, int idleCPU, int userCPU, int systemCPU, int hardIrqCPU,
			int softIrqCPU, long receivedBytes, long transmittedBytes) {
		super();
		this.ioWaitCPU = ioWaitCPU;
		this.idleCPU = idleCPU;
		this.userCPU = userCPU;
		this.systemCPU = systemCPU;
		this.hardIrqCPU = hardIrqCPU;
		this.softIrqCPU = softIrqCPU;
		this.receivedBytes = receivedBytes;
		this.transmittedBytes = transmittedBytes;
	}


	public CheckpointProfilingData(int ioWaitCPU2, int idleCPU2, int userCPU2, int systemCPU2, int hardIrqCPU2,
			int softIrqCPU2, long receivedBytes2, long transmittedBytes2, int allReadRecords, int allWrittenRecords) {
		super();
		this.ioWaitCPU = ioWaitCPU2;
		this.idleCPU = idleCPU2;
		this.userCPU = userCPU2;
		this.systemCPU = systemCPU2;
		this.hardIrqCPU = hardIrqCPU2;
		this.softIrqCPU = softIrqCPU2;
		this.receivedBytes = receivedBytes2;
		this.transmittedBytes = transmittedBytes2;
		this.allReadRecords = allReadRecords;
		this.allWrittenRecords = allWrittenRecords;
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


	public int getSystemCPU() {
		return this.systemCPU;
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

	public int getIoWaitCPU() {
		return ioWaitCPU;
	}


	public int getAllReadRecords() {
		return allReadRecords;
	}


	public int getAllWrittenRecords() {
		return allWrittenRecords;
	}





	@Override
	public void read(DataInput in) throws IOException {

		
		this.ioWaitCPU = in.readInt();
		this.idleCPU = in.readInt();
		this.systemCPU = in.readInt();
		this.userCPU = in.readInt();
		this.receivedBytes = in.readLong();
		this.transmittedBytes = in.readLong();
		this.hardIrqCPU = in.readInt();
		this.softIrqCPU = in.readInt();
		this.allReadRecords = in.readInt();
		this.allWrittenRecords = in.readInt();
		

	}

	@Override
	public void write(DataOutput out) throws IOException {

		
		out.writeInt(this.ioWaitCPU);
		out.writeInt(this.idleCPU);
		out.writeInt(this.systemCPU);
		out.writeInt(this.userCPU);
		out.writeLong(this.receivedBytes);
		out.writeLong(this.transmittedBytes);
		out.writeInt(this.hardIrqCPU);
		out.writeInt(this.softIrqCPU);
		out.writeInt(this.allReadRecords);
		out.writeInt(this.allWrittenRecords);

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

	public void setUserCPU(int userCPU) {
		this.userCPU = userCPU;
	}

	public void setReceivedBytes(long receivedBytes) {
		this.receivedBytes = receivedBytes;
	}

	public void setTransmittedBytes(long transmittedBytes) {
		this.transmittedBytes = transmittedBytes;
	}
	
	public void setAllReadRecords(int allReadRecords) {
		this.allReadRecords = allReadRecords;
	}


	public void setAllWrittenRecords(int allWrittenRecords) {
		this.allWrittenRecords = allWrittenRecords;
	}
}