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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * This class is responsible to hold operator Uid hashes from the common operators of the sink. With
 * this, users can recover a sink snapshot that did not bind uids to the operator before changing
 * the topology.
 */
@PublicEvolving
public class CustomSinkOperatorUidHashes {

    /** Default instance providing no custom sink operator hashes. */
    public static final CustomSinkOperatorUidHashes DEFAULT =
            CustomSinkOperatorUidHashes.builder().build();

    @Nullable private final String writerUidHash;
    @Nullable private final String committerUidHash;
    @Nullable private final String globalCommitterUidHash;

    private CustomSinkOperatorUidHashes(
            @Nullable String writerUidHash,
            @Nullable String committerUidHash,
            @Nullable String globalCommitterUidHash) {
        this.writerUidHash = writerUidHash;
        this.committerUidHash = committerUidHash;
        this.globalCommitterUidHash = globalCommitterUidHash;
    }

    /**
     * Creates a builder to construct {@link CustomSinkOperatorUidHashes}.
     *
     * @return {@link SinkOperatorUidHashesBuilder}
     */
    public static SinkOperatorUidHashesBuilder builder() {
        return new SinkOperatorUidHashesBuilder();
    }

    @Internal
    @Nullable
    public String getWriterUidHash() {
        return writerUidHash;
    }

    @Internal
    @Nullable
    public String getCommitterUidHash() {
        return committerUidHash;
    }

    @Internal
    @Nullable
    public String getGlobalCommitterUidHash() {
        return globalCommitterUidHash;
    }

    /** Builder to construct {@link CustomSinkOperatorUidHashes}. */
    @PublicEvolving
    public static class SinkOperatorUidHashesBuilder {

        @Nullable String writerUidHash = null;
        @Nullable String committerUidHash = null;
        @Nullable String globalCommitterUidHash = null;

        /**
         * Sets the uid hash of the writer operator used to recover state.
         *
         * @param writerUidHash uid hash denoting writer operator
         * @return {@link SinkOperatorUidHashesBuilder}
         */
        public SinkOperatorUidHashesBuilder setWriterUidHash(String writerUidHash) {
            this.writerUidHash = writerUidHash;
            return this;
        }

        /**
         * Sets the uid hash of the committer operator used to recover state.
         *
         * @param committerUidHash uid hash denoting the committer operator
         * @return {@link SinkOperatorUidHashesBuilder}
         */
        public SinkOperatorUidHashesBuilder setCommitterUidHash(String committerUidHash) {
            this.committerUidHash = committerUidHash;
            return this;
        }

        /**
         * Sets the uid hash of the global committer operator used to recover state.
         *
         * @param globalCommitterUidHash uid hash denoting the global committer operator
         * @return {@link SinkOperatorUidHashesBuilder}
         */
        public SinkOperatorUidHashesBuilder setGlobalCommitterUidHash(
                String globalCommitterUidHash) {
            this.globalCommitterUidHash = globalCommitterUidHash;
            return this;
        }

        /**
         * Constructs the {@link CustomSinkOperatorUidHashes} with the given uid hashes.
         *
         * @return {@link CustomSinkOperatorUidHashes}
         */
        public CustomSinkOperatorUidHashes build() {
            return new CustomSinkOperatorUidHashes(
                    writerUidHash, committerUidHash, globalCommitterUidHash);
        }
    }
}
