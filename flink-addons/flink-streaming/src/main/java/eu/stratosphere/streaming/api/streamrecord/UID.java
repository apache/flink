/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.streaming.api.streamrecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import eu.stratosphere.core.io.IOReadableWritable;

public class UID implements IOReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private ByteBuffer uid;
	private static Random random = new Random();

	public UID() {
		uid = ByteBuffer.allocate(20);
	}

	// TODO: consider sequential ids
	public UID(int channelID) {
		byte[] uuid = new byte[16];
		random.nextBytes(uuid);
		uid = ByteBuffer.allocate(20).putInt(channelID).put(uuid);
	}

	UID(byte[] id) {
		// TODO: throw wrong length exception
		uid = ByteBuffer.wrap(id);
	}

	public int getChannelId() {
		uid.position(0);
		return uid.getInt();
	}

	public byte[] getGeneratedId() {
		uid.position(4);
		return uid.slice().array();
	}

	public byte[] getId() {
		uid.position(0);
		return uid.array();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(uid.array());
	}

	@Override
	public void read(DataInput in) throws IOException {
		byte[] uidByteArray = new byte[20];
		in.readFully(uidByteArray, 0, 20);
		uid = ByteBuffer.wrap(uidByteArray);
	}

	@Override
	public String toString() {
		return getChannelId() + "-" + Long.toHexString(uid.getLong(4)) + "-" + Long.toHexString(uid.getLong(12));
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(getId());
	}

	@Override
	public boolean equals(Object obj) {
		try {
			UID other = (UID) obj;
			return Arrays.equals(this.getId(), other.getId());
		} catch (ClassCastException e) {
			return false;
		}
	}

	public UID copy() {
		return new UID(Arrays.copyOf(uid.array(), 20));
	}
}
