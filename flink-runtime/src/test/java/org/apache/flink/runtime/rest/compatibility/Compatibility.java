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

package org.apache.flink.runtime.rest.compatibility;

/**
 * Compatibility-qualifier for the REST API.
 */
enum Compatibility {
	/**
	 * The API is not backward compatible, meaning that the API was modified in a breaking way.
	 */
	INCOMPATIBLE,
	/**
	 * The API is backward compatible, meaning that the API was modified but not in a breaking way.
	 */
	COMPATIBLE,
	/**
	 * The API is identical to a previous version. Exists primarily to determine whether the snapshot file must be updated.
	 */
	IDENTICAL;

	/**
	 * Merges 2 compatibilities by prioritizing the most restrictive one.
	 */
	Compatibility merge(final Compatibility other) {
		if (this == INCOMPATIBLE || other == INCOMPATIBLE) {
			return INCOMPATIBLE;
		}
		if (this == COMPATIBLE || other == COMPATIBLE) {
			return COMPATIBLE;
		}
		return IDENTICAL;
	}
}
