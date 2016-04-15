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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.UUID;

public class ValueID implements Value, Comparable<ValueID> {
	private static final long serialVersionUID = -562791433077971752L;

	private UUID id;

	public ValueID() {
		id = UUID.randomUUID();
	}

	public ValueID(UUID id) {
		this.id = id;
	}

	@Override
	public int compareTo(ValueID o) {
		return id.compareTo(o.id);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(id.getMostSignificantBits());
		out.writeLong(id.getLeastSignificantBits());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		id = new UUID(in.readLong(), in.readLong());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueID) {
			ValueID other = (ValueID) obj;

			return id.equals(other.id);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
