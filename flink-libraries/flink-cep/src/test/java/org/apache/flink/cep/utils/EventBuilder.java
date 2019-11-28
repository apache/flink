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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.Event;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Builder for {@link Event} that is used in cep tests.
 */
public class EventBuilder {

	private final int id;
	private final double price;
	private final String name;
	private final Long timestamp;

	private EventBuilder(int id, double price, String name, Long timestamp) {
		this.id = id;
		this.price = price;
		this.name = name;
		this.timestamp = timestamp;
	}

	public static EventBuilder event() {
		return new EventBuilder(0, 0.0, "", null);
	}

	public EventBuilder withId(int id) {
		return new EventBuilder(id, price, name, timestamp);
	}

	public EventBuilder withPrice(double price) {
		return new EventBuilder(id, price, name, timestamp);
	}

	public EventBuilder withName(String name) {
		return new EventBuilder(id, price, name, timestamp);
	}

	public EventBuilder withTimestamp(long timestamp) {
		return new EventBuilder(id, price, name, timestamp);
	}

	public StreamRecord<Event> asStreamRecord() {
		if (timestamp != null) {
			return new StreamRecord<>(build(), timestamp);
		} else {
			return new StreamRecord<>(build());
		}
	}

	public Event build() {
		return new Event(id, name, price);
	}
}
