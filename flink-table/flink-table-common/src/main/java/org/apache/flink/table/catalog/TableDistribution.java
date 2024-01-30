/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.utils.EncodingUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Distribution specification. */
@PublicEvolving
public class TableDistribution {

    private final Kind kind;
    private final @Nullable Integer bucketCount;
    private final List<String> bucketKeys;

    public TableDistribution(Kind kind, @Nullable Integer bucketCount, List<String> bucketKeys) {
        this.kind = kind;
        this.bucketCount = bucketCount;
        this.bucketKeys = bucketKeys;
    }

    /**
     * Connector-dependent distribution of the given kind over the given keys with a declared number
     * of buckets.
     */
    public static TableDistribution of(
            Kind kind, @Nullable Integer bucketCount, List<String> bucketKeys) {
        return new TableDistribution(kind, bucketCount, bucketKeys);
    }

    /** Connector-dependent distribution with a declared number of buckets. */
    public static TableDistribution ofUnknown(int bucketCount) {
        return new TableDistribution(Kind.UNKNOWN, bucketCount, Collections.emptyList());
    }

    /** Connector-dependent distribution with a declared number of buckets. */
    public static TableDistribution ofUnknown(
            List<String> bucketKeys, @Nullable Integer bucketCount) {
        return new TableDistribution(Kind.UNKNOWN, bucketCount, bucketKeys);
    }

    /** Hash distribution over the given keys among the declared number of buckets. */
    public static TableDistribution ofHash(List<String> bucketKeys, @Nullable Integer bucketCount) {
        return new TableDistribution(Kind.HASH, bucketCount, bucketKeys);
    }

    /** Range distribution over the given keys among the declared number of buckets. */
    public static TableDistribution ofRange(
            List<String> bucketKeys, @Nullable Integer bucketCount) {
        return new TableDistribution(Kind.RANGE, bucketCount, bucketKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableDistribution that = (TableDistribution) o;
        return kind == that.kind
                && Objects.equals(bucketCount, that.bucketCount)
                && Objects.equals(bucketKeys, that.bucketKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, bucketCount, bucketKeys);
    }

    /** Distribution kind. */
    public enum Kind {
        UNKNOWN,
        HASH,
        RANGE
    }

    public Kind getKind() {
        return kind;
    }

    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    public Optional<Integer> getBucketCount() {
        return Optional.ofNullable(bucketCount);
    }

    public String asSerializableString() {
        if (getBucketKeys().isEmpty()
                && getBucketCount().isPresent()
                && getBucketCount().get() != 0) {
            return "DISTRIBUTED INTO " + getBucketCount().get() + " BUCKETS\n";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("DISTRIBUTED BY ");
        if (getKind() != null && getKind() != Kind.UNKNOWN) {
            sb.append(getKind());
        }
        sb.append("(");
        sb.append(
                getBucketKeys().stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")));
        sb.append(")");
        if (getBucketCount().isPresent() && getBucketCount().get() != 0) {
            sb.append(" INTO ");
            sb.append(getBucketCount().get());
            sb.append(" BUCKETS");
        }
        sb.append("\n");
        return sb.toString();
    }
}
