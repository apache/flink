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

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link RequestBody} for testing purpose.
 */
@SuppressWarnings("unused")
public class TestRequestBody implements RequestBody {

	private static final String FIELD_NAME_A = "a";
	private static final String FIELD_NAME_B = "b";

	@JsonProperty(FIELD_NAME_A)
	private final String a;

	@JsonProperty(FIELD_NAME_B)
	private final String b;

	@JsonCreator
	public TestRequestBody(
			@JsonProperty(FIELD_NAME_A) String a,
			@JsonProperty(FIELD_NAME_B) String b) {
		this.a = a;
		this.b = b;
	}

	@JsonIgnore
	public String getA() {
		return a;
	}

	@JsonIgnore
	public String getB() {
		return b;
	}
}
