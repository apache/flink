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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.plan.rules.logical.WrapJsonAggFunctionArgumentsRule;
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.RawValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.functions.SqlJsonUtils.createArrayNode;
import static org.apache.flink.table.runtime.functions.SqlJsonUtils.getNodeFactory;
import static org.apache.flink.table.runtime.functions.SqlJsonUtils.serializeJson;

/**
 * Implementation for {@link BuiltInFunctionDefinitions#JSON_ARRAYAGG_ABSENT_ON_NULL} / {@link
 * BuiltInFunctionDefinitions#JSON_ARRAYAGG_NULL_ON_NULL}.
 *
 * <p>Note that this function only ever receives strings to accumulate because {@link
 * WrapJsonAggFunctionArgumentsRule} wraps arguments into {@link
 * BuiltInFunctionDefinitions#JSON_STRING}.
 */
@Internal
public class JsonArrayAggFunction
        extends BuiltInAggregateFunction<String, JsonArrayAggFunction.Accumulator> {

    private static final long serialVersionUID = 1L;

    /**
     * Marker that represents a {@code null} since {@link ListView} does not allow {@code null}s.
     *
     * <p>Note that due to {@link WrapJsonAggFunctionArgumentsRule} and the fact that this function
     * already only receives JSON strings, this value cannot be created by the user and is thus safe
     * to use.
     */
    private static final StringData NULL_STR = StringData.fromString("null");

    private final transient List<DataType> argumentTypes;
    private final boolean skipNulls;

    public JsonArrayAggFunction(LogicalType[] argumentTypes, boolean skipNulls) {
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
                        "list", ListView.newListViewDataType(DataTypes.STRING().toInternal())));
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void resetAccumulator(Accumulator acc) {
        acc.list.clear();
    }

    public void accumulate(Accumulator acc, StringData itemData) throws Exception {
        if (itemData == null) {
            if (!skipNulls) {
                acc.list.add(NULL_STR);
            }
        } else {
            acc.list.add(itemData);
        }
    }

    public void retract(Accumulator acc, StringData itemData) throws Exception {
        if (itemData == null) {
            acc.list.remove(NULL_STR);
        } else {
            acc.list.remove(itemData);
        }
    }

    @Override
    public String getValue(Accumulator acc) {
        final ArrayNode rootNode = createArrayNode();
        try {
            for (final StringData item : acc.list.get()) {
                final JsonNode itemNode =
                        getNodeFactory().rawValueNode(new RawValue(item.toString()));
                rootNode.add(itemNode);
            }
        } catch (Exception e) {
            throw new TableException("The accumulator state could not be serialized.", e);
        }

        return serializeJson(rootNode);
    }

    // ---------------------------------------------------------------------------------------------

    /** Accumulator for {@link JsonArrayAggFunction}. */
    public static class Accumulator {

        public ListView<StringData> list = new ListView<>();

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final JsonArrayAggFunction.Accumulator that = (JsonArrayAggFunction.Accumulator) other;
            return Objects.equals(list, that.list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(list);
        }
    }
}
