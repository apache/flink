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

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import java.io.Serializable;

/** Interface for implementing an adaptive join operator. */
@Internal
public interface AdaptiveJoin extends Serializable {

    /**
     * Generates a StreamOperatorFactory for this join operator using the provided ClassLoader and
     * config.
     *
     * @param classLoader the ClassLoader to be used for loading classes.
     * @param config the configuration to be applied for creating the operator factory.
     * @return a StreamOperatorFactory instance.
     */
    StreamOperatorFactory<?> genOperatorFactory(ClassLoader classLoader, ReadableConfig config);

    /**
     * Get the join type of the join operator.
     *
     * @return the join type.
     */
    FlinkJoinType getJoinType();

    /**
     * Determine whether the adaptive join operator can be optimized as broadcast hash join and
     * decide which input side is the build side or a smaller side.
     *
     * @param canBeBroadcast whether the join operator can be optimized to broadcast hash join.
     * @param leftIsBuild whether the left input side is the build side.
     */
    void markAsBroadcastJoin(boolean canBeBroadcast, boolean leftIsBuild);

    /**
     * Check if the adaptive join node needs to adjust the read order of the input sides. For the
     * hash join operator, it is necessary to ensure that the build side is read first.
     *
     * @return whether the inputs should be reordered.
     */
    boolean shouldReorderInputs();
}
