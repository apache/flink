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

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Contains information about one log of TaskManager.
 */
public class LogInfo implements Serializable {

	public static final String NAME = "name";

	public static final String SIZE = "size";

	private static final long serialVersionUID = -7371944349031708629L;

	@JsonProperty(NAME)
	private final String name;

	@JsonProperty(SIZE)
	private final long size;

	@JsonCreator
	public LogInfo(@JsonProperty(NAME) String name, @JsonProperty(SIZE) long size) {
		this.name = Preconditions.checkNotNull(name);
		this.size = size;
	}

	@JsonIgnore
	public String getName() {
		return name;
	}

	@JsonIgnore
	public long getSize() {
		return size;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LogInfo that = (LogInfo) o;
		return Objects.equals(name, that.name) && size == that.size;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, size);
	}
}
