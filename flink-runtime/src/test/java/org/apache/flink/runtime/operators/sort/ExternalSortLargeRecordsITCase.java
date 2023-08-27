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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class ExternalSortLargeRecordsITCase {

    private static final int MEMORY_SIZE = 1024 * 1024 * 78;

    private final AbstractInvokable parentTask = new DummyInvokable();

    private IOManager ioManager;

    private MemoryManager memoryManager;

    private boolean testSuccess;

    // --------------------------------------------------------------------------------------------

    @BeforeEach
    void beforeTest() {
        this.memoryManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        this.ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        this.ioManager.close();

        if (this.memoryManager != null && testSuccess) {
            assertThat(this.memoryManager.verifyEmpty())
                    .withFailMessage(
                            "Memory leak: not all segments have been returned to the memory manager.")
                    .isTrue();
            this.memoryManager.shutdown();
            this.memoryManager = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testSortWithLongRecordsOnly() {
        try {
            final int NUM_RECORDS = 10;

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SomeMaybeLongValue>(SomeMaybeLongValue.class)
                    };

            final TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>> typeInfo =
                    new TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>>(types);
            final TypeSerializer<Tuple2<Long, SomeMaybeLongValue>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, SomeMaybeLongValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {false}, 0, new ExecutionConfig());

            MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> source =
                    new MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>>() {
                        private final Random rnd = new Random(457821643089756298L);
                        private int num = 0;

                        @Override
                        public Tuple2<Long, SomeMaybeLongValue> next(
                                Tuple2<Long, SomeMaybeLongValue> reuse) {
                            return next();
                        }

                        @Override
                        public Tuple2<Long, SomeMaybeLongValue> next() {
                            if (num++ < NUM_RECORDS) {
                                long val = rnd.nextLong();
                                return new Tuple2<Long, SomeMaybeLongValue>(
                                        val, new SomeMaybeLongValue((int) val));
                            } else {
                                return null;
                            }
                        }
                    };

            Sorter<Tuple2<Long, SomeMaybeLongValue>> sorter =
                    ExternalSorter.newBuilder(
                                    this.memoryManager, this.parentTask, serializer, comparator)
                            .maxNumFileHandles(128)
                            .sortBuffers(1)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction(1.0)
                            .objectReuse(false)
                            .largeRecords(true)
                            .build(source);

            // check order
            MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> iterator = sorter.getIterator();

            Tuple2<Long, SomeMaybeLongValue> val = serializer.createInstance();

            long prevKey = Long.MAX_VALUE;

            for (int i = 0; i < NUM_RECORDS; i++) {
                val = iterator.next(val);

                assertThat(val.f0).isLessThanOrEqualTo(prevKey);
                assertThat(val.f0.intValue()).isEqualTo(val.f1.val());
            }

            assertThat(iterator.next(val)).isNull();

            sorter.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testSortWithLongAndShortRecordsMixed() {
        try {
            final int NUM_RECORDS = 1000000;
            final int LARGE_REC_INTERVAL = 100000;

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SomeMaybeLongValue>(SomeMaybeLongValue.class)
                    };

            final TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>> typeInfo =
                    new TupleTypeInfo<Tuple2<Long, SomeMaybeLongValue>>(types);
            final TypeSerializer<Tuple2<Long, SomeMaybeLongValue>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, SomeMaybeLongValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {false}, 0, new ExecutionConfig());

            MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> source =
                    new MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>>() {
                        private final Random rnd = new Random(145610843608763871L);
                        private int num = -1;

                        @Override
                        public Tuple2<Long, SomeMaybeLongValue> next(
                                Tuple2<Long, SomeMaybeLongValue> reuse) {
                            return next();
                        }

                        @Override
                        public Tuple2<Long, SomeMaybeLongValue> next() {
                            if (++num < NUM_RECORDS) {
                                long val = rnd.nextLong();
                                return new Tuple2<Long, SomeMaybeLongValue>(
                                        val,
                                        new SomeMaybeLongValue(
                                                (int) val, num % LARGE_REC_INTERVAL == 0));
                            } else {
                                return null;
                            }
                        }
                    };

            Sorter<Tuple2<Long, SomeMaybeLongValue>> sorter =
                    ExternalSorter.newBuilder(
                                    this.memoryManager, this.parentTask, serializer, comparator)
                            .maxNumFileHandles(128)
                            .sortBuffers(1)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction(1.0)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(source);

            // check order
            MutableObjectIterator<Tuple2<Long, SomeMaybeLongValue>> iterator = sorter.getIterator();

            Tuple2<Long, SomeMaybeLongValue> val = serializer.createInstance();

            long prevKey = Long.MAX_VALUE;

            for (int i = 0; i < NUM_RECORDS; i++) {
                val = iterator.next(val);

                assertThat(val.f0)
                        .withFailMessage("Sort order violated")
                        .isLessThanOrEqualTo(prevKey);
                assertThat(val.f0.intValue())
                        .withFailMessage("Serialization of test data type incorrect")
                        .isEqualTo(val.f1.val());
            }

            assertThat(iterator.next(val)).isNull();

            sorter.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testSortWithShortMediumAndLargeRecords() {
        try {
            final int NUM_RECORDS = 50000;
            final int LARGE_REC_INTERVAL = 10000;
            final int MEDIUM_REC_INTERVAL = 500;

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SmallOrMediumOrLargeValue>(
                                SmallOrMediumOrLargeValue.class)
                    };

            final TupleTypeInfo<Tuple2<Long, SmallOrMediumOrLargeValue>> typeInfo =
                    new TupleTypeInfo<Tuple2<Long, SmallOrMediumOrLargeValue>>(types);

            final TypeSerializer<Tuple2<Long, SmallOrMediumOrLargeValue>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, SmallOrMediumOrLargeValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {false}, 0, new ExecutionConfig());

            MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>> source =
                    new MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>>() {
                        private final Random rnd = new Random(1456108743687167086L);
                        private int num = -1;

                        @Override
                        public Tuple2<Long, SmallOrMediumOrLargeValue> next(
                                Tuple2<Long, SmallOrMediumOrLargeValue> reuse) {
                            return next();
                        }

                        @Override
                        public Tuple2<Long, SmallOrMediumOrLargeValue> next() {
                            if (++num < NUM_RECORDS) {

                                int size;
                                if (num % LARGE_REC_INTERVAL == 0) {
                                    size = SmallOrMediumOrLargeValue.LARGE_SIZE;
                                } else if (num % MEDIUM_REC_INTERVAL == 0) {
                                    size = SmallOrMediumOrLargeValue.MEDIUM_SIZE;
                                } else {
                                    size = SmallOrMediumOrLargeValue.SMALL_SIZE;
                                }

                                long val = rnd.nextLong();
                                return new Tuple2<Long, SmallOrMediumOrLargeValue>(
                                        val, new SmallOrMediumOrLargeValue((int) val, size));
                            } else {
                                return null;
                            }
                        }
                    };

            Sorter<Tuple2<Long, SmallOrMediumOrLargeValue>> sorter =
                    ExternalSorter.newBuilder(
                                    this.memoryManager, this.parentTask, serializer, comparator)
                            .maxNumFileHandles(128)
                            .sortBuffers(1)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction(1.0)
                            .objectReuse(false)
                            .largeRecords(true)
                            .build(source);

            // check order
            MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>> iterator =
                    sorter.getIterator();

            Tuple2<Long, SmallOrMediumOrLargeValue> val = serializer.createInstance();

            for (int i = 0; i < NUM_RECORDS; i++) {
                val = iterator.next(val);

                assertThat(val.f0.intValue()).isEqualTo(val.f1.val());
            }

            assertThat(iterator.next(val)).isNull();

            sorter.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testSortWithMediumRecordsOnly() {
        try {
            final int NUM_RECORDS = 70;

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SmallOrMediumOrLargeValue>(
                                SmallOrMediumOrLargeValue.class)
                    };

            final TupleTypeInfo<Tuple2<Long, SmallOrMediumOrLargeValue>> typeInfo =
                    new TupleTypeInfo<Tuple2<Long, SmallOrMediumOrLargeValue>>(types);

            final TypeSerializer<Tuple2<Long, SmallOrMediumOrLargeValue>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, SmallOrMediumOrLargeValue>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {false}, 0, new ExecutionConfig());

            MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>> source =
                    new MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>>() {
                        private final Random rnd = new Random(62360187263087678L);
                        private int num = -1;

                        @Override
                        public Tuple2<Long, SmallOrMediumOrLargeValue> next(
                                Tuple2<Long, SmallOrMediumOrLargeValue> reuse) {
                            return next();
                        }

                        @Override
                        public Tuple2<Long, SmallOrMediumOrLargeValue> next() {
                            if (++num < NUM_RECORDS) {
                                long val = rnd.nextLong();
                                return new Tuple2<Long, SmallOrMediumOrLargeValue>(
                                        val,
                                        new SmallOrMediumOrLargeValue(
                                                (int) val, SmallOrMediumOrLargeValue.MEDIUM_SIZE));
                            } else {
                                return null;
                            }
                        }
                    };

            Sorter<Tuple2<Long, SmallOrMediumOrLargeValue>> sorter =
                    ExternalSorter.newBuilder(
                                    this.memoryManager, this.parentTask, serializer, comparator)
                            .maxNumFileHandles(128)
                            .sortBuffers(1)
                            .enableSpilling(ioManager, 0.7f)
                            .memoryFraction(1.0)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(source);

            // check order
            MutableObjectIterator<Tuple2<Long, SmallOrMediumOrLargeValue>> iterator =
                    sorter.getIterator();

            Tuple2<Long, SmallOrMediumOrLargeValue> val = serializer.createInstance();

            long prevKey = Long.MAX_VALUE;

            for (int i = 0; i < NUM_RECORDS; i++) {
                val = iterator.next(val);

                assertThat(val.f0).isLessThanOrEqualTo(prevKey);
                assertThat(val.f0.intValue()).isEqualTo(val.f1.val());
            }

            assertThat(iterator.next(val)).isNull();

            sorter.close();
            testSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    public static final class SomeMaybeLongValue implements org.apache.flink.types.Value {

        private static final long serialVersionUID = 1L;

        private static final byte[] BUFFER = new byte[100 * 1024 * 1024];

        static {
            for (int i = 0; i < BUFFER.length; i++) {
                BUFFER[i] = (byte) i;
            }
        }

        private int val;

        private boolean isLong;

        public SomeMaybeLongValue() {
            this.isLong = true;
        }

        public SomeMaybeLongValue(int val) {
            this.val = val;
            this.isLong = true;
        }

        public SomeMaybeLongValue(int val, boolean isLong) {
            this.val = val;
            this.isLong = isLong;
        }

        public int val() {
            return val;
        }

        public boolean isLong() {
            return isLong;
        }

        @Override
        public void read(DataInputView in) throws IOException {
            val = in.readInt();
            isLong = in.readBoolean();

            if (isLong) {
                for (int i = 0; i < BUFFER.length; i++) {
                    byte b = in.readByte();
                    assertThat(b).isEqualTo(BUFFER[i]);
                }
            }
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(val);
            out.writeBoolean(isLong);
            if (isLong) {
                out.write(BUFFER);
            }
        }

        @Override
        public int hashCode() {
            return val;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SomeMaybeLongValue)
                    && ((SomeMaybeLongValue) obj).val == this.val;
        }

        @Override
        public String toString() {
            return isLong ? "Large Value" : "Small Value";
        }
    }

    public static final class SmallOrMediumOrLargeValue implements org.apache.flink.types.Value {

        private static final long serialVersionUID = 1L;

        public static final int SMALL_SIZE = 0;
        public static final int MEDIUM_SIZE = 12 * 1024 * 1024;
        public static final int LARGE_SIZE = 100 * 1024 * 1024;

        private int val;

        private int size;

        public SmallOrMediumOrLargeValue() {
            this.size = SMALL_SIZE;
        }

        public SmallOrMediumOrLargeValue(int val) {
            this.val = val;
            this.size = SMALL_SIZE;
        }

        public SmallOrMediumOrLargeValue(int val, int size) {
            this.val = val;
            this.size = size;
        }

        public int val() {
            return val;
        }

        public int getSize() {
            return size;
        }

        @Override
        public void read(DataInputView in) throws IOException {
            val = in.readInt();
            size = in.readInt();

            for (int i = 0; i < size; i++) {
                byte b = in.readByte();
                assertThat(b).isEqualTo((byte) i);
            }
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(val);
            out.writeInt(size);

            for (int i = 0; i < size; i++) {
                out.write((byte) (i));
            }
        }

        @Override
        public int hashCode() {
            return val;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SmallOrMediumOrLargeValue) {
                SmallOrMediumOrLargeValue other = (SmallOrMediumOrLargeValue) obj;
                return other.val == this.val && other.size == this.size;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return String.format("Value %d (%d bytes)", val, size);
        }
    }
}
