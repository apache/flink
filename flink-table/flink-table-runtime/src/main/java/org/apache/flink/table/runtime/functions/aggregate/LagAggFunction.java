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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.LinkedListSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/** Lag {@link AggregateFunction}. */
public class LagAggFunction<T> extends BuiltInAggregateFunction<T, LagAggFunction.LagAcc<T>> {

    private final transient DataType[] valueDataTypes;

    public LagAggFunction(LogicalType[] valueTypes) {
        this.valueDataTypes =
                Arrays.stream(valueTypes)
                        .map(DataTypeUtils::toInternalDataType)
                        .toArray(DataType[]::new);
        // The output value can only be not null if the default input arguments include a non-null
        // default value.
        if (!valueDataTypes[0].getLogicalType().isNullable()) {
            if (valueDataTypes.length < 3
                    || (valueDataTypes.length == 3
                            && valueDataTypes[2].getLogicalType().isNullable())) {
                valueDataTypes[0] = valueDataTypes[0].nullable();
            }
        }
        if (valueDataTypes.length == 3
                && valueDataTypes[2].getLogicalType().getTypeRoot() != LogicalTypeRoot.NULL) {
            if (valueDataTypes[0].getConversionClass() != valueDataTypes[2].getConversionClass()) {
                throw new TableException(
                        String.format(
                                "Please explicitly cast default value %s to %s.",
                                valueDataTypes[2], valueDataTypes[1]));
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Arrays.asList(valueDataTypes);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                LagAcc.class,
                DataTypes.FIELD("offset", DataTypes.INT()),
                DataTypes.FIELD("defaultValue", valueDataTypes[0]),
                DataTypes.FIELD("buffer", getLinkedListType()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private DataType getLinkedListType() {
        TypeSerializer<T> serializer =
                InternalSerializers.create(getOutputDataType().getLogicalType());
        return DataTypes.RAW(
                LinkedList.class, (TypeSerializer) new LinkedListSerializer<>(serializer));
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataTypes[0];
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(LagAcc<T> acc, T value) throws Exception {
        acc.buffer.add(value);
        while (acc.buffer.size() > acc.offset + 1) {
            acc.buffer.removeFirst();
        }
    }

    public void accumulate(LagAcc<T> acc, T value, int offset) throws Exception {
        if (offset < 0) {
            throw new TableException(String.format("Offset(%d) should be positive.", offset));
        }

        acc.offset = offset;
        accumulate(acc, value);
    }

    public void accumulate(LagAcc<T> acc, T value, int offset, T defaultValue) throws Exception {
        acc.defaultValue = defaultValue;
        accumulate(acc, value, offset);
    }

    public void resetAccumulator(LagAcc<T> acc) throws Exception {
        acc.offset = 1;
        acc.defaultValue = null;
        acc.buffer.clear();
    }

    @Override
    public T getValue(LagAcc<T> acc) {
        if (acc.buffer.size() < acc.offset + 1) {
            return acc.defaultValue;
        } else if (acc.buffer.size() == acc.offset + 1) {
            return acc.buffer.getFirst();
        } else {
            throw new TableException("Too more elements: " + acc);
        }
    }

    @Override
    public LagAcc<T> createAccumulator() {
        return new LagAcc<>();
    }

    /** Accumulator for LAG. */
    public static class LagAcc<T> {
        public int offset = 1;
        public T defaultValue = null;
        public LinkedList<T> buffer = new LinkedList<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LagAcc<?> lagAcc = (LagAcc<?>) o;
            return offset == lagAcc.offset
                    && Objects.equals(defaultValue, lagAcc.defaultValue)
                    && Objects.equals(buffer, lagAcc.buffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, defaultValue, buffer);
        }
    }
}
