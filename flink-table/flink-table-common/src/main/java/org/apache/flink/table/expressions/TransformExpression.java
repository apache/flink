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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a transform expression that can be used for partitioning or other transformations.
 * It consists of a key, an optional function name, and an optional number of buckets.
 */
@PublicEvolving
public class TransformExpression {
    private final String key;
    private final Optional<String> functionName;
    private final Optional<Integer> numBucketsOpt;

    /**
     * Creates a new TransformExpression with the given key, function name, and number of buckets.
     *
     * @param key the key to be transformed
     * @param functionName the name of the transform function, can be null
     * @param numBuckets the number of buckets for bucket transforms, can be null
     */
    public TransformExpression(
            @Nonnull String key,
            @Nullable String functionName,
            @Nullable Integer numBuckets) {
        this.key = Objects.requireNonNull(key, "Key must not be null");
        this.functionName = Optional.ofNullable(functionName);
        this.numBucketsOpt = Optional.ofNullable(numBuckets);
    }

    /**
     * Returns the key to be transformed.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the name of the transform function, if present.
     *
     * @return the function name, or empty if not set
     */
    public Optional<String> getFunctionName() {
        return functionName;
    }

    /**
     * Returns the number of buckets if this is a bucket transform, or empty otherwise.
     *
     * @return the number of buckets, or empty if not a bucket transform
     */
    public Optional<Integer> getNumBucketsOpt() {
        return numBucketsOpt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformExpression that = (TransformExpression) o;
        return key.equals(that.key) &&
               functionName.equals(that.functionName) &&
               numBucketsOpt.equals(that.numBucketsOpt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, functionName, numBucketsOpt);
    }

    @Override
    public String toString() {
        if (functionName.isPresent()) {
            StringBuilder builder = new StringBuilder()
                    .append(functionName.get())
                    .append("(")
                    .append(key);
            if (numBucketsOpt.isPresent()) {
                builder.append(", ").append(numBucketsOpt.get());
            }
            return builder.append(")").toString();
        }
        return key;
    }

    /** * Checks if this TransformExpression is compatible with another TransformExpression.
     * Compatibility is defined by having the same function name and number of buckets.
     * examples:
     * - bucket(128, user_id) is compatible with bucket(128, user_id_2)
     * - year(dt) is compatible with year(dt) but not compatible with month(dt)
     *
     * TODO Support partial compatibility, e.g., bucket(128, user_id) is compatible with bucket(64, user_id_2)
     */
    public boolean isCompatible(TransformExpression other) {
        return
               this.functionName.equals(other.functionName) &&
               this.numBucketsOpt.equals(other.numBucketsOpt);
    }
}
