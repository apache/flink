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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/** A co group operator that applies the operation only on the unwrapped values on the left. */
@Internal
public class PlanLeftUnwrappingCoGroupOperator<I1, I2, OUT, K>
        extends CoGroupOperatorBase<
                Tuple2<K, I1>, I2, OUT, CoGroupFunction<Tuple2<K, I1>, I2, OUT>> {

    public PlanLeftUnwrappingCoGroupOperator(
            CoGroupFunction<I1, I2, OUT> udf,
            Keys.SelectorFunctionKeys<I1, K> key1,
            int[] key2,
            String name,
            TypeInformation<OUT> resultType,
            TypeInformation<Tuple2<K, I1>> typeInfoWithKey1,
            TypeInformation<I2> typeInfo2) {

        super(
                new TupleLeftUnwrappingCoGrouper<I1, I2, OUT, K>(udf),
                new BinaryOperatorInformation<Tuple2<K, I1>, I2, OUT>(
                        typeInfoWithKey1, typeInfo2, resultType),
                key1.computeLogicalKeyPositions(),
                key2,
                name);
    }

    private static final class TupleLeftUnwrappingCoGrouper<I1, I2, OUT, K>
            extends WrappingFunction<CoGroupFunction<I1, I2, OUT>>
            implements CoGroupFunction<Tuple2<K, I1>, I2, OUT> {

        private static final long serialVersionUID = 1L;

        private final TupleUnwrappingIterator<I1, K> iter1;

        private TupleLeftUnwrappingCoGrouper(CoGroupFunction<I1, I2, OUT> wrapped) {
            super(wrapped);

            this.iter1 = new TupleUnwrappingIterator<I1, K>();
        }

        @Override
        public void coGroup(
                Iterable<Tuple2<K, I1>> records1, Iterable<I2> records2, Collector<OUT> out)
                throws Exception {

            iter1.set(records1.iterator());
            this.wrappedFunction.coGroup(iter1, records2, out);
        }
    }
}
