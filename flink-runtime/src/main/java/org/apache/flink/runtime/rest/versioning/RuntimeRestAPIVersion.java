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

/**
 * An enum for all versions of the REST API.
 *
 * <p>REST API versions are global and thus apply to every REST component.
 *
 * <p>Changes that must result in an API version increment include but are not limited to: -
 * modification of a handler url - addition of new mandatory parameters - removal of a
 * handler/request - modifications to request/response bodies (excluding additions)
 */
public enum RuntimeRestAPIVersion implements RestAPIVersion<RuntimeRestAPIVersion> {
    // The bigger the ordinal(its position in enum declaration), the higher the level of the
    // version.
    V0(false, false), // strictly for testing purposes
    V1(true, true);

    private final boolean isDefaultVersion;

    private final boolean isStable;

    RuntimeRestAPIVersion(boolean isDefaultVersion, boolean isStable) {
        this.isDefaultVersion = isDefaultVersion;
        this.isStable = isStable;
    }

    /**
     * Returns the URL version prefix (e.g. "v1") for this version.
     *
     * @return URL version prefix
     */
    @Override
    public String getURLVersionPrefix() {
        return name().toLowerCase();
    }

    /**
     * Returns whether this version is the default REST API version.
     *
     * @return whether this version is the default
     */
    @Override
    public boolean isDefaultVersion() {
        return isDefaultVersion;
    }

    /**
     * Returns whether this version is considered stable.
     *
     * @return whether this version is stable
     */
    @Override
    public boolean isStableVersion() {
        return isStable;
    }
}
