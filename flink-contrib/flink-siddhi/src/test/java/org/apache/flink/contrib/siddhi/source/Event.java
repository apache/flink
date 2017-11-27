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

package org.apache.flink.contrib.siddhi.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Objects;

public class Event {
	private long timestamp;
	private String name;
	private double price;
	private int id;

	public double getPrice() {
		return price;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return "Event(" + id + ", " + name + ", " + price + ", " + timestamp + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Event) {
			Event other = (Event) obj;

			return name.equals(other.name) && price == other.price && id == other.id && timestamp == other.timestamp;
		} else {
			return false;
		}
	}

	public static Event of(int id, String name, double price) {
		Event event = new Event();
		event.setId(id);
		event.setName(name);
		event.setPrice(price);
		event.setTimestamp(System.currentTimeMillis());
		return event;
	}

	public static Event of(int id, String name, double price, long timestamp) {
		Event event = new Event();
		event.setId(id);
		event.setName(name);
		event.setPrice(price);
		event.setTimestamp(timestamp);
		return event;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, price, id);
	}

	public static TypeSerializer<Event> createTypeSerializer() {
		TypeInformation<Event> typeInformation = (TypeInformation<Event>) TypeExtractor.createTypeInfo(Event.class);

		return typeInformation.createSerializer(new ExecutionConfig());
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}
}
