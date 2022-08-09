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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.util.List;
import java.util.stream.Collectors;

/**
 * GenerateSeries implements the table function `generate_series(start, stop)`
 * `generate_series(start, * stop, step)` which generate a series of values, from start to stop with
 * a step size.
 */
@Internal
public class GenerateSeriesFunction extends BuiltInTableFunction<Object> {

    private static final long serialVersionUID = 1L;

    private final transient DataType outputDataType;
    private final LogicalType outputLogicalType;

    public GenerateSeriesFunction(SpecializedContext specializedContext) {
        super(BuiltInFunctionDefinitions.GENERATE_SERIES, specializedContext);

        // The output type in the context is already wrapped, however, the result of the
        // function is not. Therefore, we need a custom output type.
        final List<LogicalType> actualTypes =
                specializedContext.getCallContext().getArgumentDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());
        this.outputLogicalType = LogicalTypeMerging.findCommonType(actualTypes).get();
        this.outputDataType = DataTypes.of(outputLogicalType).toInternal();
    }

    @Override
    public DataType getOutputDataType() {
        return outputDataType;
    }

    public void eval(Number start, Number stop) {
        eval(start, stop, 1);
    }

    public void eval(Number start, Number stop, Number step) {
        if (isZero(step)) {
            throw new IllegalArgumentException("step size cannot equal zero");
        }
        double s = start.doubleValue();
        if (step.doubleValue() > 0) {
            while (s <= stop.doubleValue()) {
                collect(converter(s));
                s += step.doubleValue();
            }
        } else {
            while (s >= stop.doubleValue()) {
                collect(converter(s));
                s += step.doubleValue();
            }
        }
    }

    private Object converter(double s) {
        switch (outputLogicalType.getTypeRoot()) {
            case TINYINT:
                return Byte.valueOf((byte) s);
            case SMALLINT:
                return Short.valueOf((short) s);
            case INTEGER:
                return Integer.valueOf((int) s);
            case BIGINT:
                return Long.valueOf((long) s);
            case FLOAT:
                return Float.valueOf((float) s);
            case DOUBLE:
                return Double.valueOf(s);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + outputLogicalType.getTypeRoot());
        }
    }

    private boolean isZero(Object number) {
        if (number instanceof Byte
                || number instanceof Short
                || number instanceof Integer
                || number instanceof Long) {
            return ((Long) number).compareTo(0L) == 0;
        } else if (number instanceof Float) {
            return ((Float) number).compareTo(0.0f) == 0;
        } else if (number instanceof Double) {
            return ((Double) number).compareTo(0.0d) == 0;
        } else {
            throw new UnsupportedOperationException("Unsupported step type: " + number.getClass());
        }
    }
}
