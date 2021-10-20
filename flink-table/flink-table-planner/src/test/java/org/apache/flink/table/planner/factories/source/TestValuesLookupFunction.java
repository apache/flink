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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.RESOURCE_COUNTER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A lookup function which find matched rows with the given fields. NOTE: We have to declare it as
 * public because it will be used in code generation.
 */
public class TestValuesLookupFunction extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;
    private final Map<Row, List<Row>> data;
    private transient boolean isOpenCalled = false;

    public TestValuesLookupFunction(Map<Row, List<Row>> data) {
        this.data = data;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        RESOURCE_COUNTER.incrementAndGet();
        isOpenCalled = true;
    }

    public void eval(Object... inputs) {
        checkArgument(isOpenCalled, "open() is not called.");
        Row key = Row.of(inputs);
        if (Arrays.asList(inputs).contains(null)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Lookup key %s contains null value, which should not happen.", key));
        }
        List<Row> list = data.get(key);
        if (list != null) {
            list.forEach(this::collect);
        }
    }

    @Override
    public void close() throws Exception {
        RESOURCE_COUNTER.decrementAndGet();
    }
}
