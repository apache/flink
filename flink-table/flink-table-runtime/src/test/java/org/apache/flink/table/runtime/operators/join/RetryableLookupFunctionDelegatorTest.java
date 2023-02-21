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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableLookupFunctionDelegator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.data.StringData.fromString;

/** Harness tests for {@link RetryableLookupFunctionDelegator}. */
public class RetryableLookupFunctionDelegatorTest {

    private final LookupFunction userLookupFunc = new TestingLookupFunction();

    private final ResultRetryStrategy retryStrategy =
            ResultRetryStrategy.fixedDelayRetry(3, 10, RetryPredicates.EMPTY_RESULT_PREDICATE);

    private final RetryableLookupFunctionDelegator delegator =
            new RetryableLookupFunctionDelegator(userLookupFunc, retryStrategy);

    private static final Map<RowData, Collection<RowData>> data = new HashMap<>();

    static {
        data.put(
                GenericRowData.of(1),
                Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
        data.put(
                GenericRowData.of(3),
                Arrays.asList(
                        GenericRowData.of(3, fromString("Jark")),
                        GenericRowData.of(3, fromString("Jackson"))));
        data.put(
                GenericRowData.of(4),
                Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
    }

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType()
                    });

    @Test
    public void testLookupWithRetry() throws Exception {
        delegator.open(new FunctionContext(new MockStreamingRuntimeContext(false, 1, 1)));
        for (int i = 1; i <= 5; i++) {
            RowData key = GenericRowData.of(i);
            assertor.assertOutputEquals(
                    "output wrong",
                    Collections.singleton(data.get(key)),
                    Collections.singleton(delegator.lookup(key)));
        }
        delegator.close();
    }

    /**
     * The {@link RetryableLookupFunctionDelegatorTest.TestingLookupFunction} is a {@link
     * LookupFunction}.
     */
    public static final class TestingLookupFunction extends LookupFunction {

        private static final long serialVersionUID = 1L;

        @Override
        public Collection<RowData> lookup(RowData keyRow) throws IOException {
            return data.get(keyRow);
        }
    }
}
