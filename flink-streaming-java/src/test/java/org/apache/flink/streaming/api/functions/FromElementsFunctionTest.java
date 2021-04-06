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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.types.Value;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link org.apache.flink.streaming.api.functions.source.FromElementsFunction}. */
public class FromElementsFunctionTest {

    @Test
    public void testStrings() {
        try {
            String[] data = {"Oh", "boy", "what", "a", "show", "!"};

            FromElementsFunction<String> source =
                    new FromElementsFunction<String>(
                            BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
                            data);

            List<String> result = new ArrayList<String>();
            source.run(new ListSourceContext<String>(result));

            assertEquals(Arrays.asList(data), result);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testNonJavaSerializableType() {
        try {
            MyPojo[] data = {new MyPojo(1, 2), new MyPojo(3, 4), new MyPojo(5, 6)};

            FromElementsFunction<MyPojo> source =
                    new FromElementsFunction<MyPojo>(
                            TypeExtractor.getForClass(MyPojo.class)
                                    .createSerializer(new ExecutionConfig()),
                            data);

            List<MyPojo> result = new ArrayList<MyPojo>();
            source.run(new ListSourceContext<MyPojo>(result));

            assertEquals(Arrays.asList(data), result);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSerializationError() {
        try {
            TypeInformation<SerializationErrorType> info =
                    new ValueTypeInfo<SerializationErrorType>(SerializationErrorType.class);

            try {
                new FromElementsFunction<SerializationErrorType>(
                        info.createSerializer(new ExecutionConfig()), new SerializationErrorType());

                fail("should fail with an exception");
            } catch (IOException e) {
                assertTrue(ExceptionUtils.stringifyException(e).contains("test exception"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeSerializationError() {
        try {
            TypeInformation<DeserializeTooMuchType> info =
                    new ValueTypeInfo<DeserializeTooMuchType>(DeserializeTooMuchType.class);

            FromElementsFunction<DeserializeTooMuchType> source =
                    new FromElementsFunction<DeserializeTooMuchType>(
                            info.createSerializer(new ExecutionConfig()),
                            new DeserializeTooMuchType());

            try {
                source.run(
                        new ListSourceContext<DeserializeTooMuchType>(
                                new ArrayList<DeserializeTooMuchType>()));
                fail("should fail with an exception");
            } catch (IOException e) {
                assertTrue(
                        ExceptionUtils.stringifyException(e)
                                .contains("user-defined serialization"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCheckpointAndRestore() {
        try {
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

            final Throwable[] error = new Throwable[1];

            // run the source asynchronously
            Thread runner =
                    new Thread() {
                        @Override
                        public void run() {
                            try {
                                source.run(ctx);
                            } catch (Throwable t) {
                                error[0] = t;
                            }
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
            runner.join();

            // check for errors
            if (error[0] != null) {
                System.err.println("Error in asynchronous source runner");
                error[0].printStackTrace();
                fail("Error in asynchronous source runner");
            }

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

            assertEquals(data, checkpointData);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
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
