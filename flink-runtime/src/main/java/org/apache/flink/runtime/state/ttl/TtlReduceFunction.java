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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;

/**
 * This class wraps reducing function with TTL logic.
 *
 * @param <T> Type of the user value of state with TTL
 */
class TtlReduceFunction<T> extends AbstractTtlDecorator<ReduceFunction<T>>
        implements ReduceFunction<TtlValue<T>> {

    TtlReduceFunction(
            ReduceFunction<T> originalReduceFunction,
            StateTtlConfig config,
            TtlTimeProvider timeProvider) {
        super(originalReduceFunction, config, timeProvider);
    }

    @Override
    public TtlValue<T> reduce(TtlValue<T> value1, TtlValue<T> value2) throws Exception {
        T userValue1 = getUnexpired(value1);
        T userValue2 = getUnexpired(value2);
        if (userValue1 != null && userValue2 != null) {
            return wrapWithTs(original.reduce(userValue1, userValue2));
        } else if (userValue1 != null) {
            return rewrapWithNewTs(value1);
        } else if (userValue2 != null) {
            return rewrapWithNewTs(value2);
        } else {
            return null;
        }
    }
}
