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

package org.apache.flink.table.legacyutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;

/** {@link AggregateFunction} for {@link Byte}. */
public class ByteMaxAggFunction extends AggregateFunction<Byte, MaxAccumulator<Byte>> {

    private static final long serialVersionUID = 1233840393767061909L;

    @Override
    public MaxAccumulator<Byte> createAccumulator() {
        final MaxAccumulator<Byte> acc = new MaxAccumulator<>();
        resetAccumulator(acc);
        return acc;
    }

    public void accumulate(MaxAccumulator<Byte> acc, Byte value) {
        if (value != null) {
            if (!acc.f1 || Byte.compare(acc.f0, value) < 0) {
                acc.f0 = value;
                acc.f1 = true;
            }
        }
    }

    @Override
    public Byte getValue(MaxAccumulator<Byte> acc) {
        if (acc.f1) {
            return acc.f0;
        } else {
            return null;
        }
    }

    public void merge(MaxAccumulator<Byte> acc, Iterable<MaxAccumulator<Byte>> its) {
        its.forEach(
                a -> {
                    if (a.f1) {
                        accumulate(acc, a.f0);
                    }
                });
    }

    public void resetAccumulator(MaxAccumulator<Byte> acc) {
        acc.f0 = 0;
        acc.f1 = false;
    }

    @Override
    public TypeInformation<MaxAccumulator<Byte>> getAccumulatorType() {
        return new TupleTypeInfo(
                MaxAccumulator.class,
                BasicTypeInfo.BYTE_TYPE_INFO,
                BasicTypeInfo.BOOLEAN_TYPE_INFO);
    }
}
