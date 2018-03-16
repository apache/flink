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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response containing the blob server port.
 */
public final class BlobServerPortResponseBody implements ResponseBody {

	static final String FIELD_NAME_PORT = "port";

	/**
	 * The port of the blob server.
	 */
	@JsonProperty(FIELD_NAME_PORT)
	public final int port;

	@JsonCreator
	public BlobServerPortResponseBody(
		@JsonProperty(FIELD_NAME_PORT) int port) {

		this.port = port;
	}

	@Override
	public int hashCode() {
		return 67 * port;
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof BlobServerPortResponseBody) {
			BlobServerPortResponseBody other = (BlobServerPortResponseBody) object;
			return this.port == other.port;
		}
		return false;
	}
}
