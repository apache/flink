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

package org.apache.flink.mesos.runtime.clusterframework.resourcemanager.attributes;

import org.apache.flink.runtime.resourcemanager.attributes.ResourceAttribute;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mesos resource attribute implementation.
 */
public class MesosResourceAttribute implements ResourceAttribute {
	private final Map<String, String> attributes;

	MesosResourceAttribute(Map<String, String> attributes) {
		this.attributes = attributes;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private final Map<String, String> attributes = new HashMap<>();

		public Builder put(String key, String value) {
			attributes.put(key, value);
			return this;
		}

		public MesosResourceAttribute build() {
			return new MesosResourceAttribute(Collections.unmodifiableMap(attributes));
		}
	}
}
