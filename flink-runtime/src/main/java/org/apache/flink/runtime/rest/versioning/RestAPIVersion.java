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

package org.apache.flink.runtime.rest.versioning;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

/**
 * An enum for all versions of the REST API.
 *
 * <p>REST API versions are global and thus apply to every REST component.
 *
 * <p>Changes that must result in an API version increment include but are not limited to:
 * - modification of a handler url
 * - addition of new mandatory parameters
 * - removal of a handler/request
 * - modifications to request/response bodies (excluding additions)
 */
public enum RestAPIVersion {
	V0(0, false, false), // strictly for testing purposes
	V1(1, true, true);

	private final int versionNumber;

	private final boolean isDefaultVersion;

	private final boolean isStable;

	RestAPIVersion(int versionNumber, boolean isDefaultVersion, boolean isStable) {
		this.versionNumber = versionNumber;
		this.isDefaultVersion = isDefaultVersion;
		this.isStable = isStable;
	}

	/**
	 * Returns the URL version prefix (e.g. "v1") for this version.
	 *
	 * @return URL version prefix
	 */
	public String getURLVersionPrefix() {
		return name().toLowerCase();
	}

	/**
	 * Returns whether this version is the default REST API version.
	 *
	 * @return whether this version is the default
	 */
	public boolean isDefaultVersion() {
		return isDefaultVersion;
	}

	/**
	 * Returns whether this version is considered stable.
	 *
	 * @return whether this version is stable
	 */
	public boolean isStableVersion() {
		return isStable;
	}

	/**
	 * Converts the given URL version prefix (e.g "v1") to a {@link RestAPIVersion}.
	 *
	 * @param prefix prefix to converted
	 * @return REST API version matching the prefix
	 * @throws IllegalArgumentException if the prefix doesn't match any version
	 */
	public static RestAPIVersion fromURLVersionPrefix(String prefix) {
		return valueOf(prefix.toUpperCase());
	}

	/**
	 * Returns the latest version from the given collection.
	 *
	 * @param versions possible candidates
	 * @return latest version
	 */
	public static RestAPIVersion getLatestVersion(Collection<RestAPIVersion> versions) {
		return Collections.max(versions, new RestAPIVersionComparator());
	}

	/**
	 * Comparator for {@link RestAPIVersion} that sorts versions based on their version number, i.e. oldest to latest.
	 */
	public static class RestAPIVersionComparator implements Comparator<RestAPIVersion> {

		@Override
		public int compare(RestAPIVersion o1, RestAPIVersion o2) {
			return Integer.compare(o1.versionNumber, o2.versionNumber);
		}
	}
}
