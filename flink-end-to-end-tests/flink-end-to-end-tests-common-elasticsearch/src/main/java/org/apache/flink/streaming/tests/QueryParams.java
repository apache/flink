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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Holder class for Elasticsearch query parameters. */
public class QueryParams {
    private final String indexName;
    private final String sortField;
    private final int from;
    private final int pageLength;
    private final boolean trackTotalHits;

    private QueryParams(Builder builder) {
        indexName = builder.indexName;
        sortField = builder.sortField;
        from = builder.from;
        pageLength = builder.pageLength;
        trackTotalHits = builder.trackTotalHits;
    }

    /**
     * New {@code QueryParams} builder.
     *
     * @param indexName The index name. This parameter is mandatory.
     * @return The builder.
     */
    public static Builder newBuilder(String indexName) {
        return new Builder(indexName);
    }

    /** {@code QueryParams} builder static inner class. */
    public static final class Builder {
        private String sortField;
        private int from;
        private int pageLength;
        private boolean trackTotalHits;
        private String indexName;

        private Builder(String indexName) {
            this.indexName = checkNotNull(indexName);
        }

        /**
         * Sets the {@code sortField} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param sortField The {@code sortField} to set.
         * @return A reference to this Builder.
         */
        public Builder sortField(String sortField) {
            this.sortField = checkNotNull(sortField);
            return this;
        }

        /**
         * Sets the {@code from} and returns a reference to this Builder enabling method chaining.
         *
         * @param from The {@code from} to set.
         * @return A reference to this Builder.
         */
        public Builder from(int from) {
            this.from = from;
            return this;
        }

        /**
         * Sets the {@code pageLength} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param pageLength The {@code pageLength} to set.
         * @return A reference to this Builder.
         */
        public Builder pageLength(int pageLength) {
            this.pageLength = pageLength;
            return this;
        }

        /**
         * Sets the {@code trackTotalHits} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param trackTotalHits The {@code trackTotalHits} to set.
         * @return A reference to this Builder.
         */
        public Builder trackTotalHits(boolean trackTotalHits) {
            this.trackTotalHits = trackTotalHits;
            return this;
        }

        /**
         * Returns a {@code QueryParams} built from the parameters previously set.
         *
         * @return A {@code QueryParams} built with parameters of this {@code QueryParams.Builder}
         */
        public QueryParams build() {
            return new QueryParams(this);
        }

        /**
         * Sets the {@code indexName} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param indexName The {@code indexName} to set.
         * @return A reference to this Builder.
         */
        public Builder indexName(String indexName) {
            this.indexName = checkNotNull(indexName);
            return this;
        }
    }

    /**
     * Sort field string.
     *
     * @return The string.
     */
    public String sortField() {
        return sortField;
    }

    /**
     * From index to start the search from. Defaults to {@code 0}.
     *
     * @return The int.
     */
    public int from() {
        return from;
    }

    /**
     * Page length int.
     *
     * @return The int.
     */
    public int pageLength() {
        return pageLength;
    }

    /**
     * Track total hits boolean.
     *
     * @return The boolean.
     */
    public boolean trackTotalHits() {
        return trackTotalHits;
    }

    /**
     * Index name string.
     *
     * @return The string.
     */
    public String indexName() {
        return indexName;
    }
}
