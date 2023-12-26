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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.FileChannelOutputView;
import org.apache.flink.runtime.io.disk.SeekableFileChannelInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.Value;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class LargeRecordHandlerITCase {

    @Test
    void testRecordHandlerCompositeKey() {
        final int PAGE_SIZE = 4 * 1024;
        final int NUM_PAGES = 1000;
        final int NUM_RECORDS = 10;

        try (final IOManager ioMan = new IOManagerAsync()) {
            final MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(NUM_PAGES * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            final AbstractInvokable owner = new DummyInvokable();

            final List<MemorySegment> initialMemory = memMan.allocatePages(owner, 6);
            final List<MemorySegment> sortMemory = memMan.allocatePages(owner, NUM_PAGES - 6);

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SomeVeryLongValue>(SomeVeryLongValue.class),
                        BasicTypeInfo.BYTE_TYPE_INFO
                    };

            final TupleTypeInfo<Tuple3<Long, SomeVeryLongValue, Byte>> typeInfo =
                    new TupleTypeInfo<Tuple3<Long, SomeVeryLongValue, Byte>>(types);

            final TypeSerializer<Tuple3<Long, SomeVeryLongValue, Byte>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple3<Long, SomeVeryLongValue, Byte>> comparator =
                    typeInfo.createComparator(
                            new int[] {2, 0}, new boolean[] {true, true}, 0, new ExecutionConfig());

            LargeRecordHandler<Tuple3<Long, SomeVeryLongValue, Byte>> handler =
                    new LargeRecordHandler<Tuple3<Long, SomeVeryLongValue, Byte>>(
                            serializer,
                            comparator,
                            ioMan,
                            memMan,
                            initialMemory,
                            owner,
                            128,
                            owner.getExecutionConfig());

            assertThat(handler.hasData()).isFalse();

            // add the test data
            Random rnd = new Random();

            for (int i = 0; i < NUM_RECORDS; i++) {
                long val = rnd.nextLong();
                handler.addRecord(new Tuple3<>(val, new SomeVeryLongValue((int) val), (byte) val));
                assertThat(handler.hasData()).isTrue();
            }

            MutableObjectIterator<Tuple3<Long, SomeVeryLongValue, Byte>> sorted =
                    handler.finishWriteAndSortKeys(sortMemory);

            assertThatThrownBy(() -> handler.addRecord(new Tuple3<>(92L, null, (byte) 1)))
                    .withFailMessage("should throw an exception")
                    .isInstanceOf(IllegalStateException.class);

            Tuple3<Long, SomeVeryLongValue, Byte> previous = null;
            Tuple3<Long, SomeVeryLongValue, Byte> next;

            while ((next = sorted.next(null)) != null) {
                // key and value must be equal
                assertThat(next.f0.intValue()).isEqualTo(next.f1.val());
                assertThat(next.f0.byteValue()).isEqualTo(next.f2);

                // order must be correct
                if (previous != null) {
                    assertThat(previous.f2).isLessThanOrEqualTo(next.f2);
                    assertThat(
                                    previous.f2.byteValue() != next.f2.byteValue()
                                            || previous.f0 <= next.f0)
                            .isTrue();
                }
                previous = next;
            }

            handler.close();

            assertThat(handler.hasData()).isFalse();

            handler.close();

            assertThatThrownBy(() -> handler.addRecord(new Tuple3<>(92L, null, (byte) 1)))
                    .withFailMessage("should throw an exception")
                    .isInstanceOf(IllegalStateException.class);

            assertThat(memMan.verifyEmpty()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public static final class SomeVeryLongValue implements Value {

        private static final long serialVersionUID = 1L;

        private static final byte[] BUFFER = new byte[50000000];

        static {
            for (int i = 0; i < BUFFER.length; i++) {
                BUFFER[i] = (byte) i;
            }
        }

        private int val;

        public SomeVeryLongValue() {}

        public SomeVeryLongValue(int val) {
            this.val = val;
        }

        public int val() {
            return val;
        }

        @Override
        public void read(DataInputView in) throws IOException {
            val = in.readInt();
            for (byte bufferByte : BUFFER) {
                byte b = in.readByte();
                assertThat(b).isEqualTo(bufferByte);
            }
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(val);
            out.write(BUFFER);
        }
    }

    @Test
    void fileTest() {
        final int PAGE_SIZE = 4 * 1024;
        final int NUM_PAGES = 4;
        final int NUM_RECORDS = 10;

        FileIOChannel.ID channel = null;

        try (final IOManager ioMan = new IOManagerAsync()) {
            final MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(NUM_PAGES * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            final AbstractInvokable owner = new DummyInvokable();

            final List<MemorySegment> memory = memMan.allocatePages(owner, NUM_PAGES);

            final TypeInformation<?>[] types =
                    new TypeInformation<?>[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new ValueTypeInfo<SomeVeryLongValue>(SomeVeryLongValue.class),
                        BasicTypeInfo.BYTE_TYPE_INFO
                    };

            final TupleTypeInfo<Tuple3<Long, SomeVeryLongValue, Byte>> typeInfo =
                    new TupleTypeInfo<Tuple3<Long, SomeVeryLongValue, Byte>>(types);

            final TypeSerializer<Tuple3<Long, SomeVeryLongValue, Byte>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());

            channel = ioMan.createChannel();
            FileChannelOutputView out =
                    new FileChannelOutputView(
                            ioMan.createBlockChannelWriter(channel), memMan, memory, PAGE_SIZE);

            // add the test data
            Random rnd = new Random();
            List<Long> offsets = new ArrayList<Long>();

            for (int i = 0; i < NUM_RECORDS; i++) {
                offsets.add(out.getWriteOffset());
                long val = rnd.nextLong();
                Tuple3<Long, SomeVeryLongValue, Byte> next =
                        new Tuple3<Long, SomeVeryLongValue, Byte>(
                                val, new SomeVeryLongValue((int) val), (byte) val);
                serializer.serialize(next, out);
            }

            out.close();

            for (int i = 1; i < offsets.size(); i++) {
                assertThat(offsets.get(i)).isGreaterThan(offsets.get(i - 1));
            }

            memMan.allocatePages(owner, memory, NUM_PAGES);

            SeekableFileChannelInputView in =
                    new SeekableFileChannelInputView(
                            ioMan, channel, memMan, memory, out.getBytesInLatestSegment());

            for (int i = 0; i < NUM_RECORDS; i++) {
                in.seek(offsets.get(i));
                Tuple3<Long, SomeVeryLongValue, Byte> next = serializer.deserialize(in);

                // key and value must be equal
                assertThat(next.f0.intValue()).isEqualTo(next.f1.val());
                assertThat(next.f0.byteValue()).isEqualTo(next.f2);
            }

            in.closeAndDelete();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
