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

package org.apache.flink.docs.rest.data;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link ResponseBody} for testing purpose.
 */
@SuppressWarnings("unused")
public class TestResponseBody implements ResponseBody {

	private static final String FIELD_NAME_C = "c";
	private static final String FIELD_NAME_D = "d";

	@JsonProperty(FIELD_NAME_C)
	private final String c;

	@JsonProperty(FIELD_NAME_D)
	private final String d;

	@JsonCreator
	public TestResponseBody(
		@JsonProperty(FIELD_NAME_C) String c,
		@JsonProperty(FIELD_NAME_D) String d) {
		this.c = c;
		this.d = d;
	}

	@JsonIgnore
	public String getC() {
		return c;
	}

	@JsonIgnore
	public String getD() {
		return d;
	}
}
