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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Class containing jmx port and jmx host.
 */
public class JMXInfo implements ResponseBody {

	public static final String FIELD_NAME_JMX_PORT = "port";
	public static final String FIELD_NAME_JMX_HOST = "host";

	@JsonProperty(FIELD_NAME_JMX_PORT)
	private final Long port;

	@JsonProperty(FIELD_NAME_JMX_HOST)
	private final String host;

	@JsonCreator
	public JMXInfo(
		@JsonProperty(FIELD_NAME_JMX_HOST) String host,
		@JsonProperty(FIELD_NAME_JMX_PORT) Long port) {
		this.host = host;
		this.port = port;
	}

	public Long getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JMXInfo that = (JMXInfo) o;
		return Objects.equals(host, that.host) && Objects.equals(port, this.port);
	}

	@Override
	public int hashCode() {
		return Objects.hash(host, port);
	}
}
