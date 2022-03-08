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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.RawValue;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.functions.SqlJsonUtils.createObjectNode;
import static org.apache.flink.table.runtime.functions.SqlJsonUtils.getNodeFactory;
import static org.apache.flink.table.runtime.functions.SqlJsonUtils.serializeJson;

/**
 * Implementation for {@link BuiltInFunctionDefinitions#JSON_OBJECTAGG_NULL_ON_NULL} / {@link
 * BuiltInFunctionDefinitions#JSON_OBJECTAGG_ABSENT_ON_NULL}.
 *
 * <p>Note that this function only ever receives strings to accumulate because {@code
 * WrapJsonAggFunctionArgumentsRule} wraps arguments into {@link
 * BuiltInFunctionDefinitions#JSON_STRING}.
 */
@Internal
public class JsonObjectAggFunction
        extends BuiltInAggregateFunction<String, JsonObjectAggFunction.Accumulator> {

    private static final long serialVersionUID = 1L;
    private static final NullNode NULL_NODE = getNodeFactory().nullNode();

    private final transient List<DataType> argumentTypes;
    private final boolean skipNulls;

    public JsonObjectAggFunction(LogicalType[] argumentTypes, boolean skipNulls) {
        this.argumentTypes =
                Arrays.stream(argumentTypes)
                        .map(DataTypeUtils::toInternalDataType)
                        .collect(Collectors.toList());

        this.skipNulls = skipNulls;
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return argumentTypes;
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.STRING();
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                Accumulator.class,
                DataTypes.FIELD(
                        "map",
                        MapView.newMapViewDataType(
                                DataTypes.STRING().notNull().toInternal(),
                                DataTypes.STRING().toInternal())));
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void resetAccumulator(Accumulator acc) {
        acc.map.clear();
    }

    public void accumulate(Accumulator acc, StringData keyData, @Nullable StringData valueData)
            throws Exception {
        assertKeyNotPresent(acc, keyData);

        if (valueData == null) {
            if (!skipNulls) {
                acc.map.put(keyData, null);
            }
        } else {
            acc.map.put(keyData, valueData);
        }
    }

    public void retract(Accumulator acc, StringData keyData, @Nullable StringData valueData)
            throws Exception {
        acc.map.remove(keyData);
    }

    public void merge(Accumulator acc, Iterable<Accumulator> others) throws Exception {
        for (final Accumulator other : others) {
            for (final StringData key : other.map.keys()) {
                assertKeyNotPresent(acc, key);
                acc.map.put(key, other.map.get(key));
            }
        }
    }

    @Override
    public String getValue(Accumulator acc) {
        final ObjectNode rootNode = createObjectNode();
        try {
            for (final StringData key : acc.map.keys()) {
                final StringData value = acc.map.get(key);
                final JsonNode valueNode =
                        value == null
                                ? NULL_NODE
                                : getNodeFactory().rawValueNode(new RawValue(value.toString()));

                rootNode.set(key.toString(), valueNode);
            }
        } catch (Exception e) {
            throw new TableException("The accumulator state could not be serialized.", e);
        }

        return serializeJson(rootNode);
    }

    private static void assertKeyNotPresent(Accumulator acc, StringData keyData) throws Exception {
        if (acc.map.contains(keyData)) {
            throw new TableException(
                    String.format(
                            "Key '%s' is already present. Duplicate keys are not allowed in JSON_OBJECTAGG. Please ensure that keys are unique.",
                            keyData.toString()));
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Accumulator for {@link JsonObjectAggFunction}. */
    public static class Accumulator {

        public MapView<StringData, StringData> map = new MapView<>();

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final Accumulator that = (Accumulator) other;
            return Objects.equals(map, that.map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(map);
        }
    }
}
