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


package org.apache.flink.runtime.io.network.serialization.types;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class ByteType implements SerializationTestType {

	private byte value;

	public ByteType() {
		this.value = (byte) 0;
	}

	private ByteType(byte value) {
		this.value = value;
	}

	@Override
	public ByteType getRandom(Random rnd) {
		return new ByteType((byte) rnd.nextInt(256));
	}

	@Override
	public int length() {
		return 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeByte(this.value);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.value = in.readByte();
	}

	@Override
	public int hashCode() {
		return this.value;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ByteType) {
			ByteType other = (ByteType) obj;
			return this.value == other.value;
		} else {
			return false;
		}
	}
}
