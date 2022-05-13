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

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Tests for {@link RestAPIVersion}. */
public class RestAPIVersionTest extends TestLogger {
    @Test
    public void testGetLatest() {
        Collection<RestAPIVersion> candidates = Arrays.asList(RestAPIVersion.V0, RestAPIVersion.V1);
        Assert.assertEquals(RestAPIVersion.V1, RestAPIVersion.getLatestVersion(candidates));
    }

    @Test
    public void testSingleDefaultVersion() {
        final List<RestAPIVersion> defaultVersions =
                Arrays.stream(RestAPIVersion.values())
                        .filter(RestAPIVersion::isDefaultVersion)
                        .collect(Collectors.toList());

        Assert.assertEquals(
                "Only one RestAPIVersion should be marked as the default. Defaults: "
                        + defaultVersions,
                1,
                defaultVersions.size());
    }
}
