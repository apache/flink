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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Joiner that unwraps values from the right set before applying the join operation.
 *
 * @param <I1> type of values in the left set
 * @param <I2> type of values in the right set
 * @param <OUT> type of resulting values
 * @param <K> type of key
 */
@Internal
public final class TupleRightUnwrappingJoiner<I1, I2, OUT, K>
        extends WrappingFunction<FlatJoinFunction<I1, I2, OUT>>
        implements FlatJoinFunction<I1, Tuple2<K, I2>, OUT> {

    private static final long serialVersionUID = 1L;

    public TupleRightUnwrappingJoiner(FlatJoinFunction<I1, I2, OUT> wrapped) {
        super(wrapped);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void join(I1 value1, Tuple2<K, I2> value2, Collector<OUT> collector) throws Exception {
        wrappedFunction.join(value1, value2 == null ? null : (I2) value2.getField(1), collector);
    }
}
