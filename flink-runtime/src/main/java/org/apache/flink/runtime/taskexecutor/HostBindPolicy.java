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

package org.apache.flink.runtime.taskexecutor;

/**
 * A host binding address mechanism policy.
 */
enum HostBindPolicy {
	HOSTNAME,
	IP,
	AUTO_DETECT_HOSTNAME;

	@Override
	public String toString() {
		return name().toLowerCase().replace('_', '-');
	}

	public static HostBindPolicy fromString(String configValue) {
		switch (configValue) {
			case "hostname":
				return HOSTNAME;
			case "ip":
				return IP;
			case "auto-detect-hostname":
				return AUTO_DETECT_HOSTNAME;
			default:
				throw new IllegalArgumentException("Unknown host bind policy: " + configValue);
		}
	}
}
