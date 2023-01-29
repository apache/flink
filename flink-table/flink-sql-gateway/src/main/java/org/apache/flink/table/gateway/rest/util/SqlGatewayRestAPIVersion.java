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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An enum for all versions of the Sql Gateway REST API.
 *
 * <p>Sql Gateway REST API versions are global and thus apply to every REST component.
 *
 * <p>Changes that must result in an API version increment include but are not limited to: -
 * modification of a handler url - addition of new mandatory parameters - removal of a
 * handler/request - modifications to request/response bodies (excluding additions)
 */
public enum SqlGatewayRestAPIVersion
        implements RestAPIVersion<SqlGatewayRestAPIVersion>, EndpointVersion {
    // The bigger the ordinal(its position in enum declaration), the higher the level of the
    // version.

    // V0 is just for test
    V0(false, false),

    // V1 introduces basic APIs for rest endpoint
    V1(false, true),

    // V2 adds support for configuring Session and allows to serialize the RowData with PLAIN_TEXT
    // or JSON format.
    V2(true, true);

    private final boolean isDefaultVersion;

    private final boolean isStable;

    SqlGatewayRestAPIVersion(boolean isDefaultVersion, boolean isStable) {
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

    /**
     * Convert uri to SqlGatewayRestAPIVersion. If failed, return default version.
     *
     * @return SqlGatewayRestAPIVersion
     */
    public static SqlGatewayRestAPIVersion fromURIToVersion(String uri) {
        int slashIndex = uri.indexOf('/', 1);
        if (slashIndex < 0) {
            slashIndex = uri.length();
        }

        try {
            return valueOf(uri.substring(1, slashIndex).toUpperCase());
        } catch (Exception e) {
            return getDefaultVersion();
        }
    }

    /**
     * Returns the supported stable versions.
     *
     * @return the list of the stable versions.
     */
    public static List<SqlGatewayRestAPIVersion> getStableVersions() {
        return Arrays.stream(SqlGatewayRestAPIVersion.values())
                .filter(SqlGatewayRestAPIVersion::isStableVersion)
                .collect(Collectors.toList());
    }

    /**
     * Returns the default version.
     *
     * @return the default version.
     */
    public static SqlGatewayRestAPIVersion getDefaultVersion() {
        List<SqlGatewayRestAPIVersion> versions =
                Arrays.stream(SqlGatewayRestAPIVersion.values())
                        .filter(SqlGatewayRestAPIVersion::isDefaultVersion)
                        .collect(Collectors.toList());
        Preconditions.checkState(
                versions.size() == 1,
                String.format(
                        "Only one default version of Sql Gateway Rest API, but found %s.",
                        versions.size()));

        return versions.get(0);
    }
}
