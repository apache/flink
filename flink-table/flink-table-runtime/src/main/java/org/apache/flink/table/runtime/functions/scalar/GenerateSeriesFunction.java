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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#GENERATE_SERIES}. */
public class GenerateSeriesFunction extends BuiltInScalarFunction {
    public GenerateSeriesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.GENERATE_SERIES, context);
    }

    public @Nullable ArrayData eval(Long start, Long end) {
        try {
            if (start == null || end == null) {
                return null;
            }
            return generateSeries(start, end, (long) 1);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    public @Nullable ArrayData eval(Long start, Long end, Integer step) {
        try {
            if (start == null || end == null || step == null) {
                return null;
            }
            if (step == 0) {
                throw new FlinkRuntimeException(
                        "Step argument in GENERATE_SERIES function cannot be zero.");
            }
            return generateSeries(start, end, (long) step);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private ArrayData generateSeries(Long start, Long end, Long step) {
        List<Long> result = new ArrayList<>();
        if (step > 0) {
            for (Long i = start; i <= end; i += step) {
                result.add(i);
            }
        } else {
            for (Long i = start; i >= end; i += step) {
                result.add(i);
            }
        }
        return new GenericArrayData(result.toArray());
    }
}
