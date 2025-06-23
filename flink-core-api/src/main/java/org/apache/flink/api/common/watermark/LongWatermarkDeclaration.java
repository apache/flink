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

package org.apache.flink.api.common.watermark;

import org.apache.flink.annotation.Experimental;

import java.util.Objects;

/**
 * The {@link LongWatermarkDeclaration} class implements the {@link WatermarkDeclaration} interface
 * and provides additional functionality specific to long-type watermarks. It includes methods for
 * obtaining combination semantics and creating new long watermarks.
 */
@Experimental
public class LongWatermarkDeclaration implements WatermarkDeclaration {

    protected static final long serialVersionUID = 1L;

    protected final String identifier;

    protected final WatermarkCombinationPolicy combinationPolicy;

    protected final WatermarkHandlingStrategy defaultHandlingStrategy;

    public LongWatermarkDeclaration(
            String identifier,
            WatermarkCombinationPolicy combinationPolicy,
            WatermarkHandlingStrategy defaultHandlingStrategy) {
        this.identifier = identifier;
        this.combinationPolicy = combinationPolicy;
        this.defaultHandlingStrategy = defaultHandlingStrategy;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    /** Creates a new {@link LongWatermark} with the specified long value. */
    public LongWatermark newWatermark(long val) {
        return new LongWatermark(val, identifier);
    }

    public WatermarkCombinationPolicy getCombinationPolicy() {
        return combinationPolicy;
    }

    public WatermarkHandlingStrategy getDefaultHandlingStrategy() {
        return defaultHandlingStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongWatermarkDeclaration that = (LongWatermarkDeclaration) o;
        return Objects.equals(identifier, that.identifier)
                && Objects.equals(combinationPolicy, that.combinationPolicy)
                && defaultHandlingStrategy == that.defaultHandlingStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, combinationPolicy, defaultHandlingStrategy);
    }

    @Override
    public String toString() {
        return "LongWatermarkDeclaration{"
                + "identifier='"
                + identifier
                + '\''
                + ", combinationPolicy="
                + combinationPolicy
                + ", defaultHandlingStrategy="
                + defaultHandlingStrategy
                + '}';
    }
}
