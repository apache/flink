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

package org.apache.flink.streaming.tests.general.artificialstate.eventpayload;

import org.apache.flink.streaming.tests.general.Event;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ComplexPayload implements Serializable {

	public ComplexPayload(Event event) {
		this.eventTime = event.getEventTime();
		this.innerPayLoad = new InnerPayLoad(event.getSequenceNumber());
		this.stringList = Arrays.asList(String.valueOf(event.getKey()), event.getPayload());
	}

	private final long eventTime;
	private final List<String> stringList;
	private final InnerPayLoad innerPayLoad;

	public static class InnerPayLoad implements Serializable {
		private final long sequenceNumber;

		public InnerPayLoad(long sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}

		public long getSequenceNumber() {
			return sequenceNumber;
		}
	}

	public long getEventTime() {
		return eventTime;
	}

	public List<String> getStringList() {
		return stringList;
	}

	public InnerPayLoad getInnerPayLoad() {
		return innerPayLoad;
	}
}

