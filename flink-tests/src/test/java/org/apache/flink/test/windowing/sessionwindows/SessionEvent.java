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

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.util.Preconditions;

/**
 * Simple class that represents session events for our example job.
 *
 * @param <K> session key type
 * @param <V> session event value type (the event's payload)
 */
public final class SessionEvent<K, V> {

	// key of the session this event belongs to
	private K sessionKey;

	// value of the event (payload)
	private V eventValue;

	// event timestamp (in ms)
	private long eventTimestamp;

	public SessionEvent() {
	}

	public SessionEvent(K sessionKey, V eventValue, long eventTimestamp) {
		Preconditions.checkNotNull(sessionKey);
		Preconditions.checkNotNull(eventValue);
		setSessionKey(sessionKey);
		setEventValue(eventValue);
		setEventTimestamp(eventTimestamp);
	}

	public K getSessionKey() {
		return sessionKey;
	}

	public void setSessionKey(K sessionKey) {
		Preconditions.checkNotNull(sessionKey);
		this.sessionKey = sessionKey;
	}

	public V getEventValue() {
		return eventValue;
	}

	public void setEventValue(V eventValue) {
		Preconditions.checkNotNull(eventValue);
		this.eventValue = eventValue;
	}

	public long getEventTimestamp() {
		return eventTimestamp;
	}

	public void setEventTimestamp(long eventTimestamp) {
		this.eventTimestamp = eventTimestamp;
	}

	@Override
	public String toString() {
		return "SessionEvent{" +
				"sessionKey=" + sessionKey +
				", eventValue=" + eventValue +
				", eventTimestamp=" + eventTimestamp +
				'}';
	}

	public static <K, V> SessionEvent<K, V> of(K sessionKey, V eventValue, long eventTimestamp) {
		return new SessionEvent<>(sessionKey, eventValue, eventTimestamp);
	}

}
