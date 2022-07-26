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

package org.apache.flink.table.gateway.rest.versioning;

import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SqlGatewayRestAPIVersion}. */
public class SqlGatewayRestAPIVersionTest extends TestLogger {
    @Test
    public void testGetLatest() {
        Collection<SqlGatewayRestAPIVersion> candidates =
                Arrays.asList(SqlGatewayRestAPIVersion.V0, SqlGatewayRestAPIVersion.V1);
        assertEquals(SqlGatewayRestAPIVersion.V1, RestAPIVersion.getLatestVersion(candidates));
    }

    @Test
    public void testSingleDefaultVersion() {
        final List<SqlGatewayRestAPIVersion> defaultVersions =
                Arrays.stream(SqlGatewayRestAPIVersion.values())
                        .filter(SqlGatewayRestAPIVersion::isDefaultVersion)
                        .collect(Collectors.toList());

        assertEquals(
                1,
                defaultVersions.size(),
                "Only one SqlGatewayRestAPIVersion should be marked as the default. Defaults: "
                        + defaultVersions);
    }
}
