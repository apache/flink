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

package org.apache.flink.table.runtime.operators.aggregate.correlate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.operators.correlate.async.AsyncCorrelateRunner;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link AsyncCorrelateRunner}. */
public class AsyncCorrelateRunnerTest {

    @Test
    public void testRows() throws Exception {
        TestResultFuture resultFuture = new TestResultFuture();
        AsyncCorrelateRunner runner =
                new AsyncCorrelateRunner(
                        new GeneratedFunctionWrapper<>(new ImmediateCallbackFunction()),
                        createConverter(RowType.of(new IntType())));
        runner.setRuntimeContext(createRuntimeContext());
        runner.open((OpenContext) null);
        runner.asyncInvoke(GenericRowData.of(0), resultFuture);
        Collection<RowData> rows = resultFuture.getResult().get();
        assertThat(rows).containsExactly();

        resultFuture = new TestResultFuture();
        runner.asyncInvoke(GenericRowData.of(1), resultFuture);
        rows = resultFuture.getResult().get();
        assertThat(rows)
                .containsExactly(new JoinedRowData(GenericRowData.of(1), GenericRowData.of(1)));

        resultFuture = new TestResultFuture();
        runner.asyncInvoke(GenericRowData.of(2), resultFuture);
        rows = resultFuture.getResult().get();
        assertThat(rows)
                .containsExactly(
                        new JoinedRowData(GenericRowData.of(2), GenericRowData.of(10)),
                        new JoinedRowData(GenericRowData.of(2), GenericRowData.of(20)));
    }

    @Test
    public void testException() throws Exception {
        TestResultFuture resultFuture = new TestResultFuture();
        AsyncCorrelateRunner runner =
                new AsyncCorrelateRunner(
                        new GeneratedFunctionWrapper<>(new ExceptionFunction()),
                        createConverter(RowType.of(new IntType())));
        runner.setRuntimeContext(createRuntimeContext());
        runner.open((OpenContext) null);
        runner.asyncInvoke(GenericRowData.of(0), resultFuture);

        assertThatThrownBy(() -> resultFuture.getResult().get())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error!!!");

        TestResultFuture otherResultFuture = new TestResultFuture();
        runner.asyncInvoke(GenericRowData.of(1), otherResultFuture);
        assertThatThrownBy(() -> otherResultFuture.getResult().get())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Other Error!");
    }

    private RuntimeContext createRuntimeContext() {
        return new RuntimeUDFContext(
                new TaskInfoImpl("", 1, 0, 1, 0),
                getClass().getClassLoader(),
                new ExecutionConfig(),
                new HashMap<>(),
                new HashMap<>(),
                UnregisteredMetricsGroup.createOperatorMetricGroup());
    }

    private DataStructureConverter<RowData, Object> createConverter(LogicalType logicalType) {
        return cast(
                DataStructureConverters.getConverter(
                        TypeConversions.fromLogicalToDataType(logicalType)));
    }

    @SuppressWarnings("unchecked")
    private DataStructureConverter<RowData, Object> cast(
            DataStructureConverter<Object, Object> converter) {
        return (DataStructureConverter<RowData, Object>) (Object) converter;
    }

    /** Test Function. */
    public static class ImmediateCallbackFunction implements AsyncFunction<RowData, Object> {
        public ImmediateCallbackFunction() {}

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) throws Exception {
            List<Object> result = new ArrayList<>();
            int val = input.getInt(0);
            if (val == 1) {
                result.add(Row.of(1));
            } else if (val > 1) {
                result.add(Row.of(10));
                result.add(Row.of(20));
            }
            resultFuture.complete(result);
        }
    }

    /** Error Function. */
    public static class ExceptionFunction implements AsyncFunction<RowData, Object> {

        @Override
        public void asyncInvoke(RowData input, ResultFuture<Object> resultFuture) throws Exception {
            int val = input.getInt(0);

            if (val == 0) {
                throw new RuntimeException("Error!!!!");
            } else {
                resultFuture.completeExceptionally(new RuntimeException("Other Error!"));
            }
        }
    }

    /** Test result future. */
    public static final class TestResultFuture implements ResultFuture<RowData> {

        CompletableFuture<Collection<RowData>> data = new CompletableFuture<>();

        @Override
        public void complete(Collection<RowData> result) {
            data.complete(result);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            data.completeExceptionally(error);
        }

        public CompletableFuture<Collection<RowData>> getResult() {
            return data;
        }

        @Override
        public void complete(CollectionSupplier<RowData> supplier) {
            try {
                data.complete(supplier.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
