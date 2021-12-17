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

package org.apache.flink.streaming.api.operators.windowing.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.util.Collector;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.AllOf.allOf;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/** Tests for {@link InternalWindowFunction}. */
public class InternalWindowFunctionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalIterableAllWindowFunction() throws Exception {

        AllWindowFunctionMock mock = mock(AllWindowFunctionMock.class);
        InternalIterableAllWindowFunction<Long, String, TimeWindow> windowFunction =
                new InternalIterableAllWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Iterable<Long> i = (Iterable<Long>) mock(Iterable.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);

        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        windowFunction.process(((byte) 0), w, ctx, i, c);
        verify(mock).apply(w, i, c);

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalIterableProcessAllWindowFunction() throws Exception {

        ProcessAllWindowFunctionMock mock = mock(ProcessAllWindowFunctionMock.class);
        InternalIterableProcessAllWindowFunction<Long, String, TimeWindow> windowFunction =
                new InternalIterableProcessAllWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Iterable<Long> i = (Iterable<Long>) mock(Iterable.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);

        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);
        windowFunction.process(((byte) 0), w, ctx, i, c);
        verify(mock).process((ProcessAllWindowFunctionMock.Context) anyObject(), eq(i), eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalIterableWindowFunction() throws Exception {

        WindowFunctionMock mock = mock(WindowFunctionMock.class);
        InternalIterableWindowFunction<Long, String, Long, TimeWindow> windowFunction =
                new InternalIterableWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Iterable<Long> i = (Iterable<Long>) mock(Iterable.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);

        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);
        windowFunction.process(42L, w, ctx, i, c);
        verify(mock).apply(eq(42L), eq(w), eq(i), eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalIterableProcessWindowFunction() throws Exception {

        ProcessWindowFunctionMock mock = mock(ProcessWindowFunctionMock.class);
        InternalIterableProcessWindowFunction<Long, String, Long, TimeWindow> windowFunction =
                new InternalIterableProcessWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Iterable<Long> i = (Iterable<Long>) mock(Iterable.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        doAnswer(
                        new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                ProcessWindowFunctionMock.Context c =
                                        (ProcessWindowFunction.Context)
                                                invocationOnMock.getArguments()[1];
                                c.currentProcessingTime();
                                c.currentWatermark();
                                c.windowState();
                                c.globalState();
                                return null;
                            }
                        })
                .when(mock)
                .process(eq(42L), (ProcessWindowFunctionMock.Context) anyObject(), eq(i), eq(c));

        windowFunction.process(42L, w, ctx, i, c);
        verify(ctx).currentProcessingTime();
        verify(ctx).currentWatermark();
        verify(ctx).windowState();
        verify(ctx).globalState();

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalSingleValueWindowFunction() throws Exception {

        WindowFunctionMock mock = mock(WindowFunctionMock.class);
        InternalSingleValueWindowFunction<Long, String, Long, TimeWindow> windowFunction =
                new InternalSingleValueWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);

        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        windowFunction.process(42L, w, ctx, 23L, c);
        verify(mock)
                .apply(
                        eq(42L),
                        eq(w),
                        (Iterable<Long>) argThat(IsIterableContainingInOrder.contains(23L)),
                        eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalSingleValueAllWindowFunction() throws Exception {

        AllWindowFunctionMock mock = mock(AllWindowFunctionMock.class);
        InternalSingleValueAllWindowFunction<Long, String, TimeWindow> windowFunction =
                new InternalSingleValueAllWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);

        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        windowFunction.process(((byte) 0), w, ctx, 23L, c);
        verify(mock)
                .apply(
                        eq(w),
                        (Iterable<Long>) argThat(IsIterableContainingInOrder.contains(23L)),
                        eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalSingleValueProcessAllWindowFunction() throws Exception {

        ProcessAllWindowFunctionMock mock = mock(ProcessAllWindowFunctionMock.class);
        InternalSingleValueProcessAllWindowFunction<Long, String, TimeWindow> windowFunction =
                new InternalSingleValueProcessAllWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);

        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        windowFunction.process(((byte) 0), w, ctx, 23L, c);
        verify(mock)
                .process(
                        (ProcessAllWindowFunctionMock.Context) anyObject(),
                        (Iterable<Long>) argThat(IsIterableContainingInOrder.contains(23L)),
                        eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalSingleValueProcessWindowFunction() throws Exception {

        ProcessWindowFunctionMock mock = mock(ProcessWindowFunctionMock.class);
        InternalSingleValueProcessWindowFunction<Long, String, Long, TimeWindow> windowFunction =
                new InternalSingleValueProcessWindowFunction<>(mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        doAnswer(
                        new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                ProcessWindowFunctionMock.Context c =
                                        (ProcessWindowFunction.Context)
                                                invocationOnMock.getArguments()[1];
                                c.currentProcessingTime();
                                c.currentWatermark();
                                c.windowState();
                                c.globalState();
                                return null;
                            }
                        })
                .when(mock)
                .process(
                        eq(42L),
                        (ProcessWindowFunctionMock.Context) anyObject(),
                        (Iterable<Long>) argThat(IsIterableContainingInOrder.contains(23L)),
                        eq(c));

        windowFunction.process(42L, w, ctx, 23L, c);
        verify(ctx).currentProcessingTime();
        verify(ctx).currentWatermark();
        verify(ctx).windowState();
        verify(ctx).globalState();

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalAggregateProcessWindowFunction() throws Exception {

        AggregateProcessWindowFunctionMock mock = mock(AggregateProcessWindowFunctionMock.class);

        InternalAggregateProcessWindowFunction<
                        Long, Set<Long>, Map<Long, Long>, String, Long, TimeWindow>
                windowFunction =
                        new InternalAggregateProcessWindowFunction<>(
                                new AggregateFunction<Long, Set<Long>, Map<Long, Long>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Set<Long> createAccumulator() {
                                        return new HashSet<>();
                                    }

                                    @Override
                                    public Set<Long> add(Long value, Set<Long> accumulator) {
                                        accumulator.add(value);
                                        return accumulator;
                                    }

                                    @Override
                                    public Map<Long, Long> getResult(Set<Long> accumulator) {
                                        Map<Long, Long> result = new HashMap<>();
                                        for (Long in : accumulator) {
                                            result.put(in, in);
                                        }
                                        return result;
                                    }

                                    @Override
                                    public Set<Long> merge(Set<Long> a, Set<Long> b) {
                                        a.addAll(b);
                                        return a;
                                    }
                                },
                                mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);

        List<Long> args = new LinkedList<>();
        args.add(23L);
        args.add(24L);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        doAnswer(
                        new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                ProcessWindowFunctionMock.Context c =
                                        (ProcessWindowFunction.Context)
                                                invocationOnMock.getArguments()[1];
                                c.currentProcessingTime();
                                c.currentWatermark();
                                c.windowState();
                                c.globalState();
                                return null;
                            }
                        })
                .when(mock)
                .process(
                        eq(42L),
                        (AggregateProcessWindowFunctionMock.Context) anyObject(),
                        (Iterable)
                                argThat(
                                        containsInAnyOrder(
                                                allOf(
                                                        hasEntry(is(23L), is(23L)),
                                                        hasEntry(is(24L), is(24L))))),
                        eq(c));

        windowFunction.process(42L, w, ctx, args, c);
        verify(ctx).currentProcessingTime();
        verify(ctx).currentWatermark();
        verify(ctx).windowState();
        verify(ctx).globalState();

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInternalAggregateProcessAllWindowFunction() throws Exception {

        AggregateProcessAllWindowFunctionMock mock =
                mock(AggregateProcessAllWindowFunctionMock.class);

        InternalAggregateProcessAllWindowFunction<
                        Long, Set<Long>, Map<Long, Long>, String, TimeWindow>
                windowFunction =
                        new InternalAggregateProcessAllWindowFunction<>(
                                new AggregateFunction<Long, Set<Long>, Map<Long, Long>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Set<Long> createAccumulator() {
                                        return new HashSet<>();
                                    }

                                    @Override
                                    public Set<Long> add(Long value, Set<Long> accumulator) {
                                        accumulator.add(value);
                                        return accumulator;
                                    }

                                    @Override
                                    public Map<Long, Long> getResult(Set<Long> accumulator) {
                                        Map<Long, Long> result = new HashMap<>();
                                        for (Long in : accumulator) {
                                            result.put(in, in);
                                        }
                                        return result;
                                    }

                                    @Override
                                    public Set<Long> merge(Set<Long> a, Set<Long> b) {
                                        a.addAll(b);
                                        return a;
                                    }
                                },
                                mock);

        // check setOutputType
        TypeInformation<String> stringType = BasicTypeInfo.STRING_TYPE_INFO;
        ExecutionConfig execConf = new ExecutionConfig();
        execConf.setParallelism(42);

        StreamingFunctionUtils.setOutputType(windowFunction, stringType, execConf);
        verify(mock).setOutputType(stringType, execConf);

        // check open
        Configuration config = new Configuration();

        windowFunction.open(config);
        verify(mock).open(config);

        // check setRuntimeContext
        RuntimeContext rCtx = mock(RuntimeContext.class);

        windowFunction.setRuntimeContext(rCtx);
        verify(mock).setRuntimeContext(rCtx);

        // check apply
        TimeWindow w = mock(TimeWindow.class);
        Collector<String> c = (Collector<String>) mock(Collector.class);

        List<Long> args = new LinkedList<>();
        args.add(23L);
        args.add(24L);
        InternalWindowFunction.InternalWindowContext ctx =
                mock(InternalWindowFunction.InternalWindowContext.class);

        windowFunction.process(((byte) 0), w, ctx, args, c);
        verify(mock)
                .process(
                        (AggregateProcessAllWindowFunctionMock.Context) anyObject(),
                        (Iterable)
                                argThat(
                                        containsInAnyOrder(
                                                allOf(
                                                        hasEntry(is(23L), is(23L)),
                                                        hasEntry(is(24L), is(24L))))),
                        eq(c));

        // check close
        windowFunction.close();
        verify(mock).close();
    }

    private static class ProcessWindowFunctionMock
            extends ProcessWindowFunction<Long, String, Long, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void process(
                Long aLong,
                ProcessWindowFunction<Long, String, Long, TimeWindow>.Context context,
                Iterable<Long> elements,
                Collector<String> out)
                throws Exception {}
    }

    private static class AggregateProcessWindowFunctionMock
            extends ProcessWindowFunction<Map<Long, Long>, String, Long, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void process(
                Long aLong,
                ProcessWindowFunction<Map<Long, Long>, String, Long, TimeWindow>.Context context,
                Iterable<Map<Long, Long>> elements,
                Collector<String> out)
                throws Exception {}
    }

    private static class AggregateProcessAllWindowFunctionMock
            extends ProcessAllWindowFunction<Map<Long, Long>, String, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void process(Context context, Iterable<Map<Long, Long>> input, Collector<String> out)
                throws Exception {}
    }

    private static class WindowFunctionMock
            extends RichWindowFunction<Long, String, Long, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void apply(Long aLong, TimeWindow w, Iterable<Long> input, Collector<String> out)
                throws Exception {}
    }

    private static class AllWindowFunctionMock
            extends RichAllWindowFunction<Long, String, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void apply(TimeWindow window, Iterable<Long> values, Collector<String> out)
                throws Exception {}
    }

    private static class ProcessAllWindowFunctionMock
            extends ProcessAllWindowFunction<Long, String, TimeWindow>
            implements OutputTypeConfigurable<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void setOutputType(
                TypeInformation<String> outTypeInfo, ExecutionConfig executionConfig) {}

        @Override
        public void process(Context context, Iterable<Long> input, Collector<String> out)
                throws Exception {}
    }
}
