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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 */
public class LeaderInfo implements ResponseBody {

	public static final String FIELD_ADDRESS = "address";
	public static final String FIELD_DISPATCHER_ID = "dispatcher-id";

	@JsonProperty(FIELD_ADDRESS)
	private String address;

	@JsonProperty(FIELD_DISPATCHER_ID)
	private String dispatcherId;

	@JsonCreator
	public LeaderInfo(
		@JsonProperty(FIELD_ADDRESS) String address,
		@JsonProperty(FIELD_DISPATCHER_ID) String dispatcherId
	) {
		this.address = checkNotNull(address);
		this.dispatcherId = checkNotNull(dispatcherId);
	}

	@JsonIgnore
	public String getAddress() {
		return address;
	}

	@JsonIgnore
	public String getDispatcherId() {
		return dispatcherId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LeaderInfo that = (LeaderInfo) o;
		return Objects.equals(address, that.address) &&
			Objects.equals(dispatcherId, that.dispatcherId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(address, dispatcherId);
	}
}
