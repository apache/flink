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

package org.apache.flink;

import org.apache.flink.annotation.Public;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enumeration for Flink versions.
 *
 * <p>It used for API versioning, during SQL/Table API upgrades, and for migration tests.
 */
@Public
public enum FlinkVersion {

    // NOTE: the version strings must not change,
    // as they are used to locate snapshot file paths.
    // The definition order (enum ordinal) matters for performing version arithmetic.
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
    v1_14("1.14"),
    v1_15("1.15"),
    v1_16("1.16"),
    v1_17("1.17"),
    v1_18("1.18"),
    v1_19("1.19");

    private final String versionStr;

    FlinkVersion(String versionStr) {
        this.versionStr = versionStr;
    }

    @Override
    public String toString() {
        return versionStr;
    }

    public boolean isNewerVersionThan(FlinkVersion otherVersion) {
        return this.ordinal() > otherVersion.ordinal();
    }

    /** Returns all versions within the defined range, inclusive both start and end. */
    public static Set<FlinkVersion> rangeOf(FlinkVersion start, FlinkVersion end) {
        return Stream.of(FlinkVersion.values())
                .filter(v -> v.ordinal() >= start.ordinal() && v.ordinal() <= end.ordinal())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static final Map<String, FlinkVersion> CODE_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(v -> v.versionStr, Function.identity()));

    public static Optional<FlinkVersion> byCode(String code) {
        return Optional.ofNullable(CODE_MAP.get(code));
    }

    public static FlinkVersion valueOf(int majorVersion, int minorVersion) {
        return FlinkVersion.valueOf("v" + majorVersion + "_" + minorVersion);
    }

    /** Returns the version for the current branch. */
    public static FlinkVersion current() {
        return values()[values().length - 1];
    }
}
