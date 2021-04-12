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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * A reduce operator that takes 2-tuples (key-value pairs), and applies the reduce operation only on
 * the unwrapped values.
 */
@Internal
public class PlanUnwrappingReduceOperator<T, K>
        extends ReduceOperatorBase<Tuple2<K, T>, ReduceFunction<Tuple2<K, T>>> {

    public PlanUnwrappingReduceOperator(
            ReduceFunction<T> udf,
            Keys.SelectorFunctionKeys<T, K> key,
            String name,
            TypeInformation<T> type,
            TypeInformation<Tuple2<K, T>> typeInfoWithKey) {
        super(
                new ReduceWrapper<T, K>(udf),
                new UnaryOperatorInformation<Tuple2<K, T>, Tuple2<K, T>>(
                        typeInfoWithKey, typeInfoWithKey),
                key.computeLogicalKeyPositions(),
                name);
    }

    private static final class ReduceWrapper<T, K> extends WrappingFunction<ReduceFunction<T>>
            implements ReduceFunction<Tuple2<K, T>> {
        private static final long serialVersionUID = 1L;

        private ReduceWrapper(ReduceFunction<T> wrapped) {
            super(wrapped);
        }

        @Override
        public Tuple2<K, T> reduce(Tuple2<K, T> value1, Tuple2<K, T> value2) throws Exception {
            value1.f1 = this.wrappedFunction.reduce(value1.f1, value2.f1);
            return value1;
        }
    }
}
