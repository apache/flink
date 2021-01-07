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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava18.com.google.common.collect.ComparisonChain;

import org.apache.pulsar.client.api.Range;

import javax.validation.constraints.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * SerializableRange.
 */
public class SerializableRange implements Externalizable, Comparable<SerializableRange> {

	public static int fullRangeStart = 0;
	public static int fullRangeEnd = 65535;

	private Range range;

	public Range getPulsarRange() {
		return range;
	}

	public SerializableRange(Range range) {
		this.range = range;
	}

	public SerializableRange(int start, int end) {
		this.range = Range.of(start, end);
	}

	// for deserialization only
	public SerializableRange() {
		this(null);
	}

	public static SerializableRange of(Range range) {
		return new SerializableRange(range);
	}

	public static SerializableRange of(int start, int end) {
		return new SerializableRange(start, end);
	}

	public static SerializableRange ofFullRange() {
		return new SerializableRange(fullRangeStart, fullRangeEnd);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(range.getStart());
		out.writeInt(range.getEnd());
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int start = in.readInt();
		int end = in.readInt();
		this.range = Range.of(start, end);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
			.add("range", range)
			.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SerializableRange)) {
			return false;
		}
		SerializableRange that = (SerializableRange) o;
		return Objects.equals(range.getStart(), that.range.getStart())
			&& Objects.equals(range.getEnd(), that.range.getEnd());
	}

	@Override
	public int hashCode() {
		return Objects.hash(range.getStart(), range.getEnd());
	}

	@Override
	public int compareTo(@NotNull SerializableRange o) {
		return ComparisonChain.start()
			.compare(range.getStart(), o.range.getStart())
			.compare(range.getEnd(), o.range.getEnd())
			.result();
	}
}
