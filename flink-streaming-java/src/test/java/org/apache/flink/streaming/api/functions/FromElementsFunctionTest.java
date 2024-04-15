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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link org.apache.flink.streaming.api.functions.source.FromElementsFunction}. */
class FromElementsFunctionTest {

    private static final String[] STRING_ARRAY_DATA = {"Oh", "boy", "what", "a", "show", "!"};
    private static final List<String> STRING_LIST_DATA = Arrays.asList(STRING_ARRAY_DATA);

    private static <T> List<T> runSource(FromElementsFunction<T> source) throws Exception {
        List<T> result = new ArrayList<>();
        FromElementsFunction<T> clonedSource = InstantiationUtil.clone(source);
        clonedSource.run(new ListSourceContext<>(result));
        return result;
    }

    @Test
    void testStrings() throws Exception {
        String[] data = {"Oh", "boy", "what", "a", "show", "!"};

        FromElementsFunction<String> source =
                new FromElementsFunction<>(
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new SerializerConfigImpl()),
                        data);

        List<String> result = new ArrayList<>();
        source.run(new ListSourceContext<>(result));

        assertThat(result).containsExactly(data);
    }

    @Test
    void testNullElement() {
        assertThatThrownBy(() -> new FromElementsFunction<>("a", null, "b"))
                .hasMessageContaining("contains a null element")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetOutputTypeWithNoSerializer() throws Exception {
        FromElementsFunction<String> source = new FromElementsFunction<>(STRING_ARRAY_DATA);

        assertThat(source.getSerializer()).isNull();

        source.setOutputType(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig());

        assertThat(source.getSerializer())
                .isNotNull()
                .isEqualTo(
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                new SerializerConfigImpl()));

        List<String> result = runSource(source);

        assertThat(result).containsExactly(STRING_ARRAY_DATA);
    }

    @Test
    void testSetOutputTypeWithSameSerializer() throws Exception {
        FromElementsFunction<String> source =
                new FromElementsFunction<>(
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new SerializerConfigImpl()),
                        STRING_LIST_DATA);

        TypeSerializer<String> existingSerializer = source.getSerializer();

        source.setOutputType(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig());

        TypeSerializer<String> newSerializer = source.getSerializer();

        assertThat(newSerializer).isEqualTo(existingSerializer);

        List<String> result = runSource(source);

        assertThat(result).containsExactly(STRING_ARRAY_DATA);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSetOutputTypeWithIncompatibleType() {
        FromElementsFunction<String> source = new FromElementsFunction<>(STRING_LIST_DATA);

        assertThatThrownBy(
                        () ->
                                source.setOutputType(
                                        (TypeInformation) BasicTypeInfo.INT_TYPE_INFO,
                                        new ExecutionConfig()))
                .hasMessageContaining("not all subclasses of java.lang.Integer")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetOutputTypeWithExistingBrokenSerializer() throws Exception {
        // the original serializer throws an exception
        TypeInformation<DeserializeTooMuchType> info =
                new ValueTypeInfo<>(DeserializeTooMuchType.class);

        FromElementsFunction<DeserializeTooMuchType> source =
                new FromElementsFunction<>(
                        info.createSerializer(new SerializerConfigImpl()),
                        new DeserializeTooMuchType());

        TypeSerializer<DeserializeTooMuchType> existingSerializer = source.getSerializer();

        source.setOutputType(
                new GenericTypeInfo<>(DeserializeTooMuchType.class), new ExecutionConfig());

        TypeSerializer<DeserializeTooMuchType> newSerializer = source.getSerializer();

        assertThat(newSerializer).isNotEqualTo(existingSerializer);

        List<DeserializeTooMuchType> result = runSource(source);

        assertThat(result).hasSize(1).first().isInstanceOf(DeserializeTooMuchType.class);
    }

    @Test
    void testSetOutputTypeAfterTransferred() throws Exception {
        FromElementsFunction<String> source =
                InstantiationUtil.clone(new FromElementsFunction<>(STRING_LIST_DATA));

        assertThatThrownBy(
                        () ->
                                source.setOutputType(
                                        BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig()))
                .hasMessageContaining(
                        "The output type should've been specified before shipping the graph to the cluster")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testNoSerializer() {
        FromElementsFunction<String> source = new FromElementsFunction<>(STRING_LIST_DATA);

        assertThatThrownBy(() -> runSource(source))
                .hasMessageContaining("serializer not configured")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testNonJavaSerializableType() throws Exception {
        MyPojo[] data = {new MyPojo(1, 2), new MyPojo(3, 4), new MyPojo(5, 6)};

        FromElementsFunction<MyPojo> source =
                new FromElementsFunction<>(
                        TypeExtractor.getForClass(MyPojo.class)
                                .createSerializer(new SerializerConfigImpl()),
                        data);

        List<MyPojo> result = runSource(source);

        assertThat(result).containsExactly(data);
    }

    @Test
    void testNonJavaSerializableTypeWithSetOutputType() throws Exception {
        MyPojo[] data = {new MyPojo(1, 2), new MyPojo(3, 4), new MyPojo(5, 6)};

        FromElementsFunction<MyPojo> source = new FromElementsFunction<>(data);

        source.setOutputType(TypeExtractor.getForClass(MyPojo.class), new ExecutionConfig());

        List<MyPojo> result = runSource(source);

        assertThat(result).containsExactly(data);
    }

    @Test
    void testSerializationError() {
        TypeInformation<SerializationErrorType> info =
                new ValueTypeInfo<>(SerializationErrorType.class);

        assertThatThrownBy(
                        () ->
                                new FromElementsFunction<>(
                                        info.createSerializer(new SerializerConfigImpl()),
                                        new SerializationErrorType()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("test exception");
    }

    @Test
    void testDeSerializationError() throws Exception {
        TypeInformation<DeserializeTooMuchType> info =
                new ValueTypeInfo<>(DeserializeTooMuchType.class);

        FromElementsFunction<DeserializeTooMuchType> source =
                new FromElementsFunction<>(
                        info.createSerializer(new SerializerConfigImpl()),
                        new DeserializeTooMuchType());

        assertThatThrownBy(() -> source.run(new ListSourceContext<>(new ArrayList<>())))
                .hasMessageContaining("user-defined serialization")
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCheckpointAndRestore() throws Exception {
        final int numElements = 10000;

        List<Integer> data = new ArrayList<Integer>(numElements);
        List<Integer> result = new ArrayList<Integer>(numElements);

        for (int i = 0; i < numElements; i++) {
            data.add(i);
        }

        final FromElementsFunction<Integer> source =
                new FromElementsFunction<>(IntSerializer.INSTANCE, data);
        StreamSource<Integer, FromElementsFunction<Integer>> src = new StreamSource<>(source);
        AbstractStreamOperatorTestHarness<Integer> testHarness =
                new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
        testHarness.open();

        final SourceFunction.SourceContext<Integer> ctx =
                new ListSourceContext<Integer>(result, 2L);

        // run the source asynchronously
        CheckedThread runner =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source.run(ctx);
                    }
                };
        runner.start();

        // wait for a bit
        Thread.sleep(1000);

        // make a checkpoint
        List<Integer> checkpointData = new ArrayList<>(numElements);
        OperatorSubtaskState handles = null;
        synchronized (ctx.getCheckpointLock()) {
            handles = testHarness.snapshot(566, System.currentTimeMillis());
            checkpointData.addAll(result);
        }

        // cancel the source
        source.cancel();
        runner.sync();

        final FromElementsFunction<Integer> sourceCopy =
                new FromElementsFunction<>(IntSerializer.INSTANCE, data);
        StreamSource<Integer, FromElementsFunction<Integer>> srcCopy =
                new StreamSource<>(sourceCopy);
        AbstractStreamOperatorTestHarness<Integer> testHarnessCopy =
                new AbstractStreamOperatorTestHarness<>(srcCopy, 1, 1, 0);
        testHarnessCopy.setup();
        testHarnessCopy.initializeState(handles);
        testHarnessCopy.open();

        // recovery run
        SourceFunction.SourceContext<Integer> newCtx = new ListSourceContext<>(checkpointData);

        sourceCopy.run(newCtx);

        assertThat(checkpointData).isEqualTo(data);
    }

    // ------------------------------------------------------------------------
    //  Test Types
    // ------------------------------------------------------------------------

    private static class MyPojo {

        public long val1;
        public int val2;

        public MyPojo() {}

        public MyPojo(long val1, int val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        @Override
        public int hashCode() {
            return this.val2;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof MyPojo) {
                MyPojo that = (MyPojo) obj;
                return this.val1 == that.val1 && this.val2 == that.val2;
            } else {
                return false;
            }
        }
    }

    private static class SerializationErrorType implements Value {

        private static final long serialVersionUID = -6037206294939421807L;

        @Override
        public void write(DataOutputView out) throws IOException {
            throw new IOException("test exception");
        }

        @Override
        public void read(DataInputView in) throws IOException {
            throw new IOException("test exception");
        }
    }

    private static class DeserializeTooMuchType implements Value {

        private static final long serialVersionUID = -6037206294939421807L;

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(42);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            in.readLong();
        }
    }
}
