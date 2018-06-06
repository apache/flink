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

package org.apache.flink.streaming.examples.statemachine.event;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Data type for events, consisting of the originating IP address and an event type.
 */
public class Event {

	private final EventType type;

	private final int sourceAddress;

	/**
	 * Creates a new event.
	 *
	 * @param type The event type.
	 * @param sourceAddress The originating address (think 32 bit IPv4 address).
	 */
	public Event(EventType type, int sourceAddress) {
		this.type = checkNotNull(type);
		this.sourceAddress = sourceAddress;
	}

	/**
	 * Gets the event's type.
	 */
	public EventType type() {
		return type;
	}

	/**
	 * Gets the event's source address.
	 */
	public int sourceAddress() {
		return sourceAddress;
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous
	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 31 * type.hashCode() + sourceAddress;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final Event that = (Event) obj;
			return this.type == that.type && this.sourceAddress == that.sourceAddress;
		}
	}

	@Override
	public String toString() {
		return "Event " + formatAddress(sourceAddress) + " : " + type.name();
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	/**
	 * Util method to create a string representation of a 32 bit integer representing
	 * an IPv4 address.
	 *
	 * @param address The address, MSB first.
	 * @return The IP address string.
	 */
	public static String formatAddress(int address) {
		int b1 = (address >>> 24) & 0xff;
		int b2 = (address >>> 16) & 0xff;
		int b3 = (address >>>  8) & 0xff;
		int b4 =  address         & 0xff;

		return "" + b1 + '.' + b2 + '.' + b3 + '.' + b4;
	}
}
