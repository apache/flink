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

package org.apache.flink.runtime.net.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.Objects;

/**
 * A REST error response entity.
 */
@JsonPropertyOrder({"code", "detail"})
public class RestError implements Serializable {

	private static final long serialVersionUID = 1L;

	private int code;
	private String detail;

	@JsonCreator
	public RestError(@JsonProperty("code") int code, @JsonProperty("detail") String detail) {
		this.code = code;
		this.detail = detail;
	}

	@JsonProperty("code")
	public int code() {
		return code;
	}

	@JsonProperty("detail")
	public String detail() {
		return detail;
	}

	@Override
	public String toString() {
		return "RestError{" +
			"code=" + code +
			", detail='" + detail + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RestError restError = (RestError) o;
		return code == restError.code &&
			Objects.equals(detail, restError.detail);
	}

	@Override
	public int hashCode() {
		return Objects.hash(code, detail);
	}
}
