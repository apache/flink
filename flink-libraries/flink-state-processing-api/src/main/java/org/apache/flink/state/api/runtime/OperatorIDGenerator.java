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

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Utility for generating Operator Id's from uid strings. */
@Internal
public final class OperatorIDGenerator {
    private OperatorIDGenerator() {}

    /**
     * Generate {@link OperatorID}'s from {@code uid}'s.
     *
     * <p>{@link
     * org.apache.flink.streaming.api.graph.StreamGraphHasherV2#traverseStreamGraphAndGenerateHashes(StreamGraph)})}
     *
     * @param uid {@code DataStream} operator uid.
     * @return corresponding {@link OperatorID}
     */
    public static OperatorID fromUid(String uid) {
        byte[] hash = Hashing.murmur3_128(0).newHasher().putString(uid, UTF_8).hash().asBytes();
        return new OperatorID(hash);
    }
}
