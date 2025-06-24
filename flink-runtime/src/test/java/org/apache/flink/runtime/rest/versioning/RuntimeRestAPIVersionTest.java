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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RuntimeRestAPIVersion}. */
class RuntimeRestAPIVersionTest {
    @Test
    void testGetLatest() {
        Collection<RuntimeRestAPIVersion> candidates =
                Arrays.asList(RuntimeRestAPIVersion.V0, RuntimeRestAPIVersion.V1);
        assertThat(RestAPIVersion.getLatestVersion(candidates)).isEqualTo(RuntimeRestAPIVersion.V1);
    }

    @Test
    void testSingleDefaultVersion() {
        final List<RuntimeRestAPIVersion> defaultVersions =
                Arrays.stream(RuntimeRestAPIVersion.values())
                        .filter(RuntimeRestAPIVersion::isDefaultVersion)
                        .collect(Collectors.toList());

        assertThat(defaultVersions.size())
                .as(
                        "Only one RestAPIVersion should be marked as the default. Defaults: "
                                + defaultVersions)
                .isOne();
    }
}
