/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/**
 * Interface for implementing an adaptive broadcast join operator. This interface allows a join
 * operator to be dynamically optimized to a broadcast join during runtime if a specific input side
 * meets the conditions. If not, the join operator can revert to its original implementation.
 */
@Internal
public interface AdaptiveJoin extends Serializable {

    /**
     * Generates a StreamOperatorFactory for the join operator using the provided ClassLoader and
     * config.
     *
     * @param classLoader the ClassLoader to be used for loading classes.
     * @param config the configuration to be applied for creating the operator factory.
     * @return a StreamOperatorFactory instance.
     */
    StreamOperatorFactory<?> genOperatorFactory(ClassLoader classLoader, ReadableConfig config);

    /**
     * Enrich the input data sizes and checks for broadcast support.
     *
     * @param leftInputBytes The size of the left input in bytes.
     * @param rightInputBytes The size of the right input in bytes.
     * @param threshold The threshold for enabling broadcast hash join.
     * @return A Tuple2 instance. The first element of tuple is true if join can convert to
     *     broadcast hash join, false else. The second element of tuple is true if left side is
     *     smaller, false else.
     */
    Tuple2<Boolean, Boolean> enrichAndCheckBroadcast(
            long leftInputBytes, long rightInputBytes, long threshold);
}
