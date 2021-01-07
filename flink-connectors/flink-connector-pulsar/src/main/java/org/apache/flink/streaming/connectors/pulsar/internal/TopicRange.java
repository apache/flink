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

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava18.com.google.common.collect.ComparisonChain;

import org.apache.pulsar.client.api.Range;

import javax.validation.constraints.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import static org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange.fullRangeEnd;
import static org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange.fullRangeStart;

/**
 * topic key_share range.
 */
@Internal
public class TopicRange implements Externalizable, Comparable<TopicRange> {

	private String topic;

	private SerializableRange range;

	// For deserialization only
	public TopicRange() {
		this(null, null);
	}

	public TopicRange(String topic) {
		this.topic = topic;
		this.range = SerializableRange.of(fullRangeStart, fullRangeEnd);
	}

	public TopicRange(String topic, Range range) {
		this.topic = topic;
		this.range = SerializableRange.of(range);
	}

	public TopicRange(String topic, int start, int end) {
		this.topic = topic;
		this.range = SerializableRange.of(start, end);
	}

	public String getTopic() {
		return topic;
	}

	public SerializableRange getRange() {
		return range;
	}

	public Range getPulsarRange() {
		return range.getPulsarRange();
	}

	public boolean isFullRange() {
		return range.getPulsarRange().getStart() == fullRangeStart &&
			range.getPulsarRange().getEnd() == fullRangeEnd;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setRange(SerializableRange range) {
		this.range = range;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof TopicRange)) {
			return false;
		}
		TopicRange that = (TopicRange) o;
		return topic.equals(that.topic) &&
			range.equals(that.range);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic, range);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
			.add("topic", topic)
			.add("key-range", range)
			.toString();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(topic);
		out.writeObject(range);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.topic = in.readUTF();
		this.range = (SerializableRange) in.readObject();
	}

	@Override
	public int compareTo(@NotNull TopicRange o) {
		return ComparisonChain.start()
			.compare(topic, o.topic)
			.compare(range, o.range)
			.result();
	}
}
