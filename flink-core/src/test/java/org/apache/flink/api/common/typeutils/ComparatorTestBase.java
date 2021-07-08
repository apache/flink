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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract test base for comparators.
 *
 * @param <T>
 */
public abstract class ComparatorTestBase<T> extends TestLogger {

    // Same as in the NormalizedKeySorter
    private static final int DEFAULT_MAX_NORMALIZED_KEY_LEN = 8;

    protected Order[] getTestedOrder() {
        return new Order[] {Order.ASCENDING, Order.DESCENDING};
    }

    protected abstract TypeComparator<T> createComparator(boolean ascending);

    protected abstract TypeSerializer<T> createSerializer();

    /**
     * Returns the sorted data set.
     *
     * <p>Note: every element needs to be *strictly greater* than the previous element.
     *
     * @return sorted test data set
     */
    protected abstract T[] getSortedTestData();

    // -------------------------------- test duplication ------------------------------------------

    @Test
    public void testDuplicate() {
        try {
            boolean ascending = isAscending(getTestedOrder()[0]);
            TypeComparator<T> comparator = getComparator(ascending);
            TypeComparator<T> clone = comparator.duplicate();

            T[] data = getSortedData();
            comparator.setReference(data[0]);
            clone.setReference(data[1]);

            assertTrue(
                    "Comparator duplication does not work: Altering the reference in a duplicated comparator alters the original comparator's reference.",
                    comparator.equalToReference(data[0]) && clone.equalToReference(data[1]));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    // --------------------------------- equality tests -------------------------------------------

    @Test
    public void testEquality() {
        for (Order order : getTestedOrder()) {
            boolean ascending = isAscending(order);
            testEquals(ascending);
        }
    }

    protected void testEquals(boolean ascending) {
        try {
            // Just setup two identical output/inputViews and go over their data to see if compare
            // works
            TestOutputView out1;
            TestOutputView out2;
            TestInputView in1;
            TestInputView in2;

            // Now use comparator and compare
            TypeComparator<T> comparator = getComparator(ascending);
            T[] data = getSortedData();
            for (T d : data) {

                out2 = new TestOutputView();
                writeSortedData(d, out2);
                in2 = out2.getInputView();

                out1 = new TestOutputView();
                writeSortedData(d, out1);
                in1 = out1.getInputView();

                assertTrue(comparator.compareSerialized(in1, in2) == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    public void testEqualityWithReference() {
        try {
            TypeSerializer<T> serializer = createSerializer();
            boolean ascending = isAscending(getTestedOrder()[0]);
            TypeComparator<T> comparator = getComparator(ascending);
            TypeComparator<T> comparator2 = getComparator(ascending);
            T[] data = getSortedData();
            for (T d : data) {
                comparator.setReference(d);
                // Make a copy to compare
                T copy = serializer.copy(d, serializer.createInstance());

                // And then test equalTo and compareToReference method of comparator
                assertTrue(comparator.equalToReference(d));
                comparator2.setReference(copy);
                assertTrue(comparator.compareToReference(comparator2) == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    // --------------------------------- inequality tests ----------------------------------------
    @Test
    public void testInequality() {
        for (Order order : getTestedOrder()) {
            boolean ascending = isAscending(order);
            testGreatSmallAscDesc(ascending, true);
            testGreatSmallAscDesc(ascending, false);
        }
    }

    protected void testGreatSmallAscDesc(boolean ascending, boolean greater) {
        try {
            // split data into low and high part
            T[] data = getSortedData();

            TypeComparator<T> comparator = getComparator(ascending);
            TestOutputView out1;
            TestOutputView out2;
            TestInputView in1;
            TestInputView in2;

            // compares every element in high with every element in low
            for (int x = 0; x < data.length - 1; x++) {
                for (int y = x + 1; y < data.length; y++) {
                    out1 = new TestOutputView();
                    writeSortedData(data[x], out1);
                    in1 = out1.getInputView();

                    out2 = new TestOutputView();
                    writeSortedData(data[y], out2);
                    in2 = out2.getInputView();

                    if (greater && ascending) {
                        assertTrue(comparator.compareSerialized(in1, in2) < 0);
                    }
                    if (greater && !ascending) {
                        assertTrue(comparator.compareSerialized(in1, in2) > 0);
                    }
                    if (!greater && ascending) {
                        assertTrue(comparator.compareSerialized(in2, in1) > 0);
                    }
                    if (!greater && !ascending) {
                        assertTrue(comparator.compareSerialized(in2, in1) < 0);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    public void testInequalityWithReference() {
        for (Order order : getTestedOrder()) {
            boolean ascending = isAscending(order);
            testGreatSmallAscDescWithReference(ascending, true);
            testGreatSmallAscDescWithReference(ascending, false);
        }
    }

    protected void testGreatSmallAscDescWithReference(boolean ascending, boolean greater) {
        try {
            T[] data = getSortedData();

            TypeComparator<T> comparatorLow = getComparator(ascending);
            TypeComparator<T> comparatorHigh = getComparator(ascending);

            // compares every element in high with every element in low
            for (int x = 0; x < data.length - 1; x++) {
                for (int y = x + 1; y < data.length; y++) {
                    comparatorLow.setReference(data[x]);
                    comparatorHigh.setReference(data[y]);

                    if (greater && ascending) {
                        assertTrue(comparatorLow.compareToReference(comparatorHigh) > 0);
                    }
                    if (greater && !ascending) {
                        assertTrue(comparatorLow.compareToReference(comparatorHigh) < 0);
                    }
                    if (!greater && ascending) {
                        assertTrue(comparatorHigh.compareToReference(comparatorLow) < 0);
                    }
                    if (!greater && !ascending) {
                        assertTrue(comparatorHigh.compareToReference(comparatorLow) > 0);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    // --------------------------------- Normalized key tests -------------------------------------

    // Help Function for setting up a memory segment and normalize the keys of the data array in it
    public MemorySegment setupNormalizedKeysMemSegment(
            T[] data, int normKeyLen, TypeComparator<T> comparator) {
        MemorySegment memSeg = MemorySegmentFactory.allocateUnpooledSegment(2048);

        // Setup normalized Keys in the memory segment
        int offset = 0;
        for (T e : data) {
            comparator.putNormalizedKey(e, memSeg, offset, normKeyLen);
            offset += normKeyLen;
        }
        return memSeg;
    }

    // Help Function which return a normalizedKeyLength, either as done in the NormalizedKeySorter
    // or it's half
    private int getNormKeyLen(boolean halfLength, T[] data, TypeComparator<T> comparator)
            throws Exception {
        // Same as in the NormalizedKeySorter
        int keyLen = Math.min(comparator.getNormalizeKeyLen(), DEFAULT_MAX_NORMALIZED_KEY_LEN);
        if (keyLen < comparator.getNormalizeKeyLen()) {
            assertTrue(comparator.isNormalizedKeyPrefixOnly(keyLen));
        }

        if (halfLength) {
            keyLen = keyLen / 2;
            assertTrue(comparator.isNormalizedKeyPrefixOnly(keyLen));
        }
        return keyLen;
    }

    @Test
    public void testNormalizedKeysEqualsFullLength() {
        // Ascending or descending does not matter in this case
        boolean ascending = isAscending(getTestedOrder()[0]);
        TypeComparator<T> comparator = getComparator(ascending);
        if (!comparator.supportsNormalizedKey()) {
            return;
        }
        testNormalizedKeysEquals(false);
    }

    @Test
    public void testNormalizedKeysEqualsHalfLength() {
        boolean ascending = isAscending(getTestedOrder()[0]);
        TypeComparator<T> comparator = getComparator(ascending);
        if (!comparator.supportsNormalizedKey()) {
            return;
        }
        testNormalizedKeysEquals(true);
    }

    public void testNormalizedKeysEquals(boolean halfLength) {
        try {
            boolean ascending = isAscending(getTestedOrder()[0]);
            TypeComparator<T> comparator = getComparator(ascending);
            T[] data = getSortedData();
            int normKeyLen = getNormKeyLen(halfLength, data, comparator);

            MemorySegment memSeg1 = setupNormalizedKeysMemSegment(data, normKeyLen, comparator);
            MemorySegment memSeg2 = setupNormalizedKeysMemSegment(data, normKeyLen, comparator);

            for (int i = 0; i < data.length; i++) {
                assertTrue(
                        memSeg1.compare(memSeg2, i * normKeyLen, i * normKeyLen, normKeyLen) == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    public void testNormalizedKeysGreatSmallFullLength() {
        // ascending/descending in comparator doesn't matter for normalized keys
        boolean ascending = isAscending(getTestedOrder()[0]);
        TypeComparator<T> comparator = getComparator(ascending);
        if (!comparator.supportsNormalizedKey()) {
            return;
        }
        testNormalizedKeysGreatSmall(true, comparator, false);
        testNormalizedKeysGreatSmall(false, comparator, false);
    }

    @Test
    public void testNormalizedKeysGreatSmallAscDescHalfLength() {
        // ascending/descending in comparator doesn't matter for normalized keys
        boolean ascending = isAscending(getTestedOrder()[0]);
        TypeComparator<T> comparator = getComparator(ascending);
        if (!comparator.supportsNormalizedKey()) {
            return;
        }
        testNormalizedKeysGreatSmall(true, comparator, true);
        testNormalizedKeysGreatSmall(false, comparator, true);
    }

    protected void testNormalizedKeysGreatSmall(
            boolean greater, TypeComparator<T> comparator, boolean halfLength) {
        try {
            T[] data = getSortedData();
            // Get the normKeyLen on which we are testing
            int normKeyLen = getNormKeyLen(halfLength, data, comparator);

            // Write the data into different 2 memory segments
            MemorySegment memSegLow = setupNormalizedKeysMemSegment(data, normKeyLen, comparator);
            MemorySegment memSegHigh = setupNormalizedKeysMemSegment(data, normKeyLen, comparator);

            boolean fullyDetermines = !comparator.isNormalizedKeyPrefixOnly(normKeyLen);

            // Compare every element with every bigger element
            for (int l = 0; l < data.length - 1; l++) {
                for (int h = l + 1; h < data.length; h++) {
                    int cmp;
                    if (greater) {
                        cmp =
                                memSegLow.compare(
                                        memSegHigh, l * normKeyLen, h * normKeyLen, normKeyLen);
                        if (fullyDetermines) {
                            assertTrue(cmp < 0);
                        } else {
                            assertTrue(cmp <= 0);
                        }
                    } else {
                        cmp =
                                memSegHigh.compare(
                                        memSegLow, h * normKeyLen, l * normKeyLen, normKeyLen);
                        if (fullyDetermines) {
                            assertTrue(cmp > 0);
                        } else {
                            assertTrue(cmp >= 0);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    public void testNormalizedKeyReadWriter() {
        try {
            T[] data = getSortedData();
            T reuse = getSortedData()[0];

            boolean ascending = isAscending(getTestedOrder()[0]);
            TypeComparator<T> comp1 = getComparator(ascending);
            if (!comp1.supportsSerializationWithKeyNormalization()) {
                return;
            }
            TypeComparator<T> comp2 = comp1.duplicate();
            comp2.setReference(reuse);

            TestOutputView out = new TestOutputView();
            TestInputView in;

            for (T value : data) {
                comp1.setReference(value);
                comp1.writeWithKeyNormalization(value, out);
                in = out.getInputView();
                comp1.readWithKeyDenormalization(reuse, in);

                assertTrue(comp1.compareToReference(comp2) == 0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    // -------------------------------- Key extraction tests --------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testKeyExtraction() {
        boolean ascending = isAscending(getTestedOrder()[0]);
        TypeComparator<T> comparator = getComparator(ascending);
        T[] data = getSortedData();

        for (T value : data) {
            TypeComparator[] comparators = comparator.getFlatComparators();
            Object[] extractedKeys = new Object[comparators.length];
            int insertedKeys = comparator.extractKeys(value, extractedKeys, 0);
            assertTrue(insertedKeys == comparators.length);

            for (int i = 0; i < insertedKeys; i++) {
                // check if some keys are null, although this is not supported
                if (!supportsNullKeys()) {
                    assertNotNull(extractedKeys[i]);
                }
                // compare the extracted key with itself as a basic check
                // if the extracted key corresponds to the comparator
                assertTrue(comparators[i].compare(extractedKeys[i], extractedKeys[i]) == 0);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    protected void deepEquals(String message, T should, T is) {
        assertEquals(message, should, is);
    }

    // --------------------------------------------------------------------------------------------
    protected TypeComparator<T> getComparator(boolean ascending) {
        TypeComparator<T> comparator = createComparator(ascending);
        if (comparator == null) {
            throw new RuntimeException("Test case corrupt. Returns null as comparator.");
        }
        return comparator;
    }

    protected T[] getSortedData() {
        T[] data = getSortedTestData();
        if (data == null) {
            throw new RuntimeException("Test case corrupt. Returns null as test data.");
        }
        if (data.length < 2) {
            throw new RuntimeException("Test case does not provide enough sorted test data.");
        }

        return data;
    }

    protected TypeSerializer<T> getSerializer() {
        TypeSerializer<T> serializer = createSerializer();
        if (serializer == null) {
            throw new RuntimeException("Test case corrupt. Returns null as serializer.");
        }
        return serializer;
    }

    protected void writeSortedData(T value, TestOutputView out) throws IOException {
        TypeSerializer<T> serializer = getSerializer();

        // Write data into a outputView
        serializer.serialize(value, out);

        // This are the same tests like in the serializer
        // Just look if the data is really there after serialization, before testing comparator on
        // it
        TestInputView in = out.getInputView();
        assertTrue("No data available during deserialization.", in.available() > 0);

        T deserialized = serializer.deserialize(serializer.createInstance(), in);
        deepEquals("Deserialized value is wrong.", value, deserialized);
    }

    protected boolean supportsNullKeys() {
        return false;
    }

    // --------------------------------------------------------------------------------------------

    private static boolean isAscending(Order order) {
        return order == Order.ASCENDING;
    }

    public static final class TestOutputView extends DataOutputStream implements DataOutputView {

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

    public static final class TestInputView extends DataInputStream implements DataInputView {

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
}
