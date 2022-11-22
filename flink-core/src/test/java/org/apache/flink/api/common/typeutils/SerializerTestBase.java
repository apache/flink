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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.testutils.CustomEqualityMatcher;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract test base for serializers.
 *
 * <p>We have a toString() call on all deserialized values because this is further evidence that the
 * deserialized value is actually correct. (JodaTime DataTime) with the default KryoSerializer used
 * to pass this test but the internal state would be corrupt, which becomes evident when toString is
 * called.
 */
@ExtendWith(TestLoggerExtension.class)
public abstract class SerializerTestBase<T> {

    private final DeeplyEqualsChecker checker;

    protected SerializerTestBase() {
        this.checker = new DeeplyEqualsChecker();
    }

    protected SerializerTestBase(DeeplyEqualsChecker checker) {
        this.checker = checker;
    }

    protected abstract TypeSerializer<T> createSerializer();

    /**
     * Gets the expected length for the serializer's {@link TypeSerializer#getLength()} method.
     *
     * <p>The expected length should be positive, for fix-length data types, or {@code -1} for
     * variable-length types.
     */
    protected abstract int getLength();

    protected abstract Class<T> getTypeClass();

    protected abstract T[] getTestData();

    /**
     * Allows {@link TypeSerializer#createInstance()} to return null.
     *
     * <p>The {@link KryoSerializer} is one example.
     */
    protected boolean allowNullInstances(TypeSerializer<T> serializer) {
        return serializer.getClass().getName().endsWith("KryoSerializer");
    }

    // --------------------------------------------------------------------------------------------

    @Test
    protected void testInstantiate() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T instance = serializer.createInstance();
            if (instance == null && allowNullInstances(serializer)) {
                return;
            }
            assertNotNull("The created instance must not be null.", instance);

            Class<T> type = getTypeClass();
            assertNotNull("The test is corrupt: type class is null.", type);

            if (!type.isAssignableFrom(instance.getClass())) {
                fail(
                        "Type of the instantiated object is wrong. "
                                + "Expected Type: "
                                + type
                                + " present type "
                                + instance.getClass());
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    protected void testConfigSnapshotInstantiation() {
        TypeSerializerSnapshot<T> configSnapshot = getSerializer().snapshotConfiguration();

        InstantiationUtil.instantiate(configSnapshot.getClass());
    }

    @Test
    protected void testSnapshotConfigurationAndReconfigure() throws Exception {
        final TypeSerializer<T> serializer = getSerializer();
        final TypeSerializerSnapshot<T> configSnapshot = serializer.snapshotConfiguration();

        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), configSnapshot);
            serializedConfig = out.toByteArray();
        }

        TypeSerializerSnapshot<T> restoredConfig;
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            restoredConfig =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        TypeSerializerSchemaCompatibility<T> strategy =
                restoredConfig.resolveSchemaCompatibility(getSerializer());
        final TypeSerializer<T> restoreSerializer;
        if (strategy.isCompatibleAsIs()) {
            restoreSerializer = restoredConfig.restoreSerializer();
        } else if (strategy.isCompatibleWithReconfiguredSerializer()) {
            restoreSerializer = strategy.getReconfiguredSerializer();
        } else {
            throw new AssertionError("Unable to restore serializer with " + strategy);
        }
        assertEquals(serializer.getClass(), restoreSerializer.getClass());
    }

    @Test
    void testGetLength() {
        final int len = getLength();

        if (len == 0) {
            fail("Broken serializer test base - zero length cannot be the expected length");
        }

        try {
            TypeSerializer<T> serializer = getSerializer();
            assertEquals(len, serializer.getLength());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    protected void testCopy() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T datum : testData) {
                T copy = serializer.copy(datum);
                checkToString(copy);
                deepEquals("Copied element is not equal to the original element.", datum, copy);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testCopyIntoNewElements() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T datum : testData) {
                T copy = serializer.copy(datum, serializer.createInstance());
                checkToString(copy);
                deepEquals("Copied element is not equal to the original element.", datum, copy);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testCopyIntoReusedElements() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            T target = serializer.createInstance();

            for (T datum : testData) {
                T copy = serializer.copy(datum, target);
                checkToString(copy);
                deepEquals("Copied element is not equal to the original element.", datum, copy);
                target = copy;
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeIndividually() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T value : testData) {
                TestOutputView out = new TestOutputView();
                serializer.serialize(value, out);
                TestInputView in = out.getInputView();

                assertTrue("No data available during deserialization.", in.available() > 0);

                T deserialized = serializer.deserialize(serializer.createInstance(), in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", value, deserialized);

                assertTrue("Trailing data available after deserialization.", in.available() == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeIndividuallyReusingValues() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            T reuseValue = serializer.createInstance();

            for (T value : testData) {
                TestOutputView out = new TestOutputView();
                serializer.serialize(value, out);
                TestInputView in = out.getInputView();

                assertTrue("No data available during deserialization.", in.available() > 0);

                T deserialized = serializer.deserialize(reuseValue, in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", value, deserialized);

                assertTrue("Trailing data available after deserialization.", in.available() == 0);

                reuseValue = deserialized;
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeAsSequenceNoReuse() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            TestOutputView out = new TestOutputView();
            for (T value : testData) {
                serializer.serialize(value, out);
            }

            TestInputView in = out.getInputView();

            int num = 0;
            while (in.available() > 0) {
                T deserialized = serializer.deserialize(in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", testData[num], deserialized);
                num++;
            }

            assertEquals("Wrong number of elements deserialized.", testData.length, num);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeAsSequenceReusingValues() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            TestOutputView out = new TestOutputView();
            for (T value : testData) {
                serializer.serialize(value, out);
            }

            TestInputView in = out.getInputView();
            T reuseValue = serializer.createInstance();

            int num = 0;
            while (in.available() > 0) {
                T deserialized = serializer.deserialize(reuseValue, in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", testData[num], deserialized);
                reuseValue = deserialized;
                num++;
            }

            assertEquals("Wrong number of elements deserialized.", testData.length, num);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializedCopyIndividually() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T value : testData) {
                TestOutputView out = new TestOutputView();
                serializer.serialize(value, out);

                TestInputView source = out.getInputView();
                TestOutputView target = new TestOutputView();
                serializer.copy(source, target);

                TestInputView toVerify = target.getInputView();

                assertTrue("No data available copying.", toVerify.available() > 0);

                T deserialized = serializer.deserialize(serializer.createInstance(), toVerify);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", value, deserialized);

                assertTrue(
                        "Trailing data available after deserialization.",
                        toVerify.available() == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializedCopyAsSequence() {
        try {
            TypeSerializer<T> serializer = getSerializer();
            T[] testData = getData();

            TestOutputView out = new TestOutputView();
            for (T value : testData) {
                serializer.serialize(value, out);
            }

            TestInputView source = out.getInputView();
            TestOutputView target = new TestOutputView();
            for (int i = 0; i < testData.length; i++) {
                serializer.copy(source, target);
            }

            TestInputView toVerify = target.getInputView();
            int num = 0;

            while (toVerify.available() > 0) {
                T deserialized = serializer.deserialize(serializer.createInstance(), toVerify);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", testData[num], deserialized);
                num++;
            }

            assertEquals("Wrong number of elements copied.", testData.length, num);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializabilityAndEquals() {
        try {
            TypeSerializer<T> ser1 = getSerializer();
            TypeSerializer<T> ser2;
            try {
                ser2 = SerializationUtils.clone(ser1);
            } catch (SerializationException e) {
                fail("The serializer is not serializable: " + e);
                return;
            }

            assertEquals(
                    "The copy of the serializer is not equal to the original one.", ser1, ser2);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testNullability() {
        TypeSerializer<T> serializer = getSerializer();
        try {
            NullableSerializer.checkIfNullSupported(serializer);
        } catch (Throwable t) {
            System.err.println(t.getMessage());
            t.printStackTrace();
            fail("Unexpected failure of null value handling: " + t.getMessage());
        }
    }

    @Test
    void testDuplicate() throws Exception {
        final int numThreads = 10;
        final TypeSerializer<T> serializer = getSerializer();
        final CyclicBarrier startLatch = new CyclicBarrier(numThreads);
        final List<SerializerRunner<T>> concurrentRunners = new ArrayList<>(numThreads);
        Assert.assertEquals(serializer, serializer.duplicate());

        T[] testData = getData();

        for (int i = 0; i < numThreads; ++i) {
            SerializerRunner<T> runner =
                    new SerializerRunner<>(
                            startLatch, serializer.duplicate(), testData, 120L, checker);

            runner.start();
            concurrentRunners.add(runner);
        }

        for (SerializerRunner<T> concurrentRunner : concurrentRunners) {
            concurrentRunner.join();
            concurrentRunner.checkResult();
        }
    }

    // --------------------------------------------------------------------------------------------

    private void deepEquals(String message, T should, T is) {
        assertThat(message, is, CustomEqualityMatcher.deeplyEquals(should).withChecker(checker));
    }

    // --------------------------------------------------------------------------------------------

    protected TypeSerializer<T> getSerializer() {
        TypeSerializer<T> serializer = createSerializer();
        if (serializer == null) {
            throw new RuntimeException("Test case corrupt. Returns null as serializer.");
        }
        return serializer;
    }

    private T[] getData() {
        T[] data = getTestData();
        if (data == null) {
            throw new RuntimeException("Test case corrupt. Returns null as test data.");
        }
        return data;
    }

    // --------------------------------------------------------------------------------------------

    private static final class TestOutputView extends DataOutputStream implements DataOutputView {

        public TestOutputView() {
            super(new ByteArrayOutputStream(4096));
        }

        public TestInputView getInputView() {
            ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
            return new TestInputView(baos.toByteArray());
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            for (int i = 0; i < numBytes; i++) {
                write(0);
            }
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            byte[] buffer = new byte[numBytes];
            source.readFully(buffer);
            write(buffer);
        }
    }

    /**
     * Runner to test serializer duplication via concurrency.
     *
     * @param <T> type of the test elements.
     */
    static class SerializerRunner<T> extends Thread {
        final CyclicBarrier allReadyBarrier;
        final TypeSerializer<T> serializer;
        final T[] testData;
        final long durationLimitMillis;
        Throwable failure;
        final DeeplyEqualsChecker checker;

        SerializerRunner(
                CyclicBarrier allReadyBarrier,
                TypeSerializer<T> serializer,
                T[] testData,
                long testTargetDurationMillis,
                DeeplyEqualsChecker checker) {

            this.allReadyBarrier = allReadyBarrier;
            this.serializer = serializer;
            this.testData = testData;
            this.durationLimitMillis = testTargetDurationMillis;
            this.checker = checker;
            this.failure = null;
        }

        @Override
        public void run() {
            DataInputDeserializer dataInputDeserializer = new DataInputDeserializer();
            DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
            try {
                allReadyBarrier.await();
                final long endTimeNanos = System.nanoTime() + durationLimitMillis * 1_000_000L;
                while (true) {
                    for (T testItem : testData) {
                        serializer.serialize(testItem, dataOutputSerializer);
                        dataInputDeserializer.setBuffer(
                                dataOutputSerializer.getSharedBuffer(),
                                0,
                                dataOutputSerializer.length());
                        T serdeTestItem = serializer.deserialize(dataInputDeserializer);
                        T copySerdeTestItem = serializer.copy(serdeTestItem);
                        dataOutputSerializer.clear();

                        assertThat(
                                "Serialization/Deserialization cycle resulted in an object that are not equal to the original.",
                                copySerdeTestItem,
                                CustomEqualityMatcher.deeplyEquals(testItem).withChecker(checker));

                        // try to enforce some upper bound to the test time
                        if (System.nanoTime() >= endTimeNanos) {
                            return;
                        }
                    }
                }
            } catch (Throwable ex) {
                failure = ex;
            }
        }

        void checkResult() throws Exception {
            if (failure != null) {
                if (failure instanceof AssertionError) {
                    throw (AssertionError) failure;
                } else {
                    throw (Exception) failure;
                }
            }
        }
    }

    private static final class TestInputView extends DataInputStream implements DataInputView {

        public TestInputView(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
            while (numBytes > 0) {
                int skipped = skipBytes(numBytes);
                numBytes -= skipped;
            }
        }
    }

    private static <T> void checkToString(T value) {
        if (value != null) {
            value.toString();
        }
    }
}
