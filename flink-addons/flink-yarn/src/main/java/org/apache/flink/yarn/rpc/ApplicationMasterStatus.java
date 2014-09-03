/**
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
package org.apache.flink.yarn.rpc;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Class holding status information about the ApplicatioMaster.
 * The client is requesting the AM status regularly from the AM.
 */
public class ApplicationMasterStatus implements IOReadableWritable {
	private int numTaskManagers = 0;
	private int numSlots = 0;
	private int messageCount = 0;
	private boolean failed = false;


	public ApplicationMasterStatus() {
		// for instantiation
	}
	
	public ApplicationMasterStatus(int numTaskManagers, int numSlots) {
		this.numTaskManagers = numTaskManagers;
		this.numSlots = numSlots;
	}

	public ApplicationMasterStatus(int numTaskManagers, int numSlots,
			int messageCount, boolean failed) {
		this(numTaskManagers, numSlots);
		this.messageCount = messageCount;
		this.failed = failed;
	}
	

	public int getNumberOfTaskManagers() {
		return numTaskManagers;
	}

	public int getNumberOfAvailableSlots() {
		return numSlots;
	}

	public int getMessageCount() {
		return messageCount;
	}
	
	public void setMessageCount(int messageCount) {
		this.messageCount = messageCount;
	}
	
	public void setFailed(Boolean isFailed) {
		this.failed = isFailed;
	}

	public void setNumTaskManagers(int num) {
		this.numTaskManagers = num;
	}
	
	public void setNumSlots(int slots) {
		this.numSlots = slots;
	}
	
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(numTaskManagers);
		out.writeInt(numSlots);
		out.writeInt(messageCount);
		out.writeBoolean(failed);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		numTaskManagers = in.readInt();
		numSlots = in.readInt();
		messageCount = in.readInt();
		failed = in.readBoolean();
	}

	public boolean getFailed() {
		return failed;
	}
}
