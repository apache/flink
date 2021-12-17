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

package org.apache.flink.testutils.migration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enumeration for Flink versions, used in migration integration tests to indicate the migrated
 * snapshot version.
 */
public enum MigrationVersion {

    // NOTE: the version strings must not change,
    // as they are used to locate snapshot file paths.
    // The definition order matters for performing version arithmetic.
    v1_3("1.3"),
    v1_4("1.4"),
    v1_5("1.5"),
    v1_6("1.6"),
    v1_7("1.7"),
    v1_8("1.8"),
    v1_9("1.9"),
    v1_10("1.10"),
    v1_11("1.11"),
    v1_12("1.12"),
    v1_13("1.13"),
    v1_14("1.14");

    private final String versionStr;

    MigrationVersion(String versionStr) {
        this.versionStr = versionStr;
    }

    @Override
    public String toString() {
        return versionStr;
    }

    public boolean isNewerVersionThan(MigrationVersion otherVersion) {
        return this.ordinal() > otherVersion.ordinal();
    }

    /** Returns all versions equal to or higher than the selected version. */
    public List<MigrationVersion> orHigher() {
        return Stream.of(MigrationVersion.values())
                .filter(v -> this.ordinal() <= v.ordinal())
                .collect(Collectors.toList());
    }

    private static final Map<String, MigrationVersion> CODE_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(v -> v.versionStr, Function.identity()));

    public static Optional<MigrationVersion> byCode(String code) {
        return Optional.ofNullable(CODE_MAP.get(code));
    }
}
