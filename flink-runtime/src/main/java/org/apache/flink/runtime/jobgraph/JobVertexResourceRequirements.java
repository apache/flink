/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Information about the parallelism of job vertices. */
public class JobVertexResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_NAME_PARALLELISM = "parallelism";

    public static class Parallelism implements Serializable {

        private static final String FIELD_NAME_LOWER_BOUND = "lowerBound";
        private static final String FIELD_NAME_UPPER_BOUND = "upperBound";

        @JsonProperty(FIELD_NAME_LOWER_BOUND)
        private final int lowerBound;

        @JsonProperty(FIELD_NAME_UPPER_BOUND)
        private final int upperBound;

        @JsonCreator
        public Parallelism(
                @JsonProperty(FIELD_NAME_LOWER_BOUND) int lowerBound,
                @JsonProperty(FIELD_NAME_UPPER_BOUND) int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public int getLowerBound() {
            return lowerBound;
        }

        public int getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Parallelism that = (Parallelism) o;
            return lowerBound == that.lowerBound && upperBound == that.upperBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowerBound, upperBound);
        }

        @Override
        public String toString() {
            return "Parallelism{" + "lowerBound=" + lowerBound + ", upperBound=" + upperBound + '}';
        }
    }

    @JsonProperty(FIELD_NAME_PARALLELISM)
    private final Parallelism parallelism;

    public JobVertexResourceRequirements(
            @JsonProperty(FIELD_NAME_PARALLELISM) Parallelism parallelism) {
        this.parallelism = checkNotNull(parallelism);
    }

    public Parallelism getParallelism() {
        return parallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobVertexResourceRequirements that = (JobVertexResourceRequirements) o;
        return parallelism.equals(that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parallelism);
    }

    @Override
    public String toString() {
        return "JobVertexResourceRequirements{" + "parallelism=" + parallelism + '}';
    }
}
