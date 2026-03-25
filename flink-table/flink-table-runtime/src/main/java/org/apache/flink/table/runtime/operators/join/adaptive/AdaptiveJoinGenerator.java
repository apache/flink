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

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import java.io.Serializable;

/** Interface for implementing an adaptive join operator generator. */
@Internal
public interface AdaptiveJoinGenerator extends Serializable {
    /**
     * Generates a StreamOperatorFactory for this join operator using the provided ClassLoader and
     * parameters.
     *
     * @param classLoader the ClassLoader to be used for loading classes.
     * @param config the configuration to be applied for creating the operator factory.
     * @param joinType the join type.
     * @param originIsSortMergeJoin whether the join operator is a SortMergeJoin.
     * @param isBroadcastJoin whether the join operator can be optimized to broadcast hash join.
     * @param leftIsBuild whether the left input side is the build side.
     * @return a StreamOperatorFactory instance.
     */
    StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader,
            ReadableConfig config,
            FlinkJoinType joinType,
            boolean originIsSortMergeJoin,
            boolean isBroadcastJoin,
            boolean leftIsBuild);
}
