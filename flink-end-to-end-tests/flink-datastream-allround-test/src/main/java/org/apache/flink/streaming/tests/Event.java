/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

/**
 * The event type of records used in the {@link DataStreamAllroundTestProgram}.
 */
public class Event {

	private final int key;
	private final long eventTime;
	private final long sequenceNumber;
	private final String payload;

	public Event(int key, long eventTime, long sequenceNumber, String payload) {
		this.key = key;
		this.eventTime = eventTime;
		this.sequenceNumber = sequenceNumber;
		this.payload = payload;
	}

	public int getKey() {
		return key;
	}

	public long getEventTime() {
		return eventTime;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public String getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "Event{" +
			"key=" + key +
			", eventTime=" + eventTime +
			", sequenceNumber=" + sequenceNumber +
			", payload='" + payload + '\'' +
			'}';
	}
}
