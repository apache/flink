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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * A simple container for a handler's JSON response and the REST URLs for which the response would've been returned.
 *
 * <p>These are created by {@link JsonArchivist}s, and used by the {@link MemoryArchivist} to create a directory structure
 * resembling the REST API.
 */
public class ArchivedJson {

	private final String path;
	private final String json;

	public ArchivedJson(String path, String json) {
		this.path = Preconditions.checkNotNull(path);
		this.json = Preconditions.checkNotNull(json);
	}

	public String getPath() {
		return path;
	}

	public String getJson() {
		return json;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ArchivedJson) {
			ArchivedJson other = (ArchivedJson) obj;
			return this.path.equals(other.path) && this.json.equals(other.json);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(path, json);
	}

	@Override
	public String toString() {
		return path + ":" + json;
	}
}
