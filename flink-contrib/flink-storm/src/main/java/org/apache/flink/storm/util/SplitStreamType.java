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

package org.apache.flink.storm.util;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Used by org.apache.flink.storm.wrappers.AbstractStormCollector to wrap
 * output tuples if multiple output streams are declared. For this case, the Flink output data stream must be split via
 * {@link DataStream#split(org.apache.flink.streaming.api.collector.selector.OutputSelector) .split(...)} using
 * {@link StormStreamSelector}.
 *
 */
public class SplitStreamType<T> {

	/** The stream ID this tuple belongs to. */
	public String streamId;
	/** The actual data value. */
	public T value;

	@Override
	public String toString() {
		return "<sid:" + this.streamId + ",v:" + this.value + ">";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SplitStreamType<?> other = (SplitStreamType<?>) o;

		return this.streamId.equals(other.streamId) && this.value.equals(other.value);
	}
}
